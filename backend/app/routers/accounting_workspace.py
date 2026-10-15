"""Internal accounting workspace: journals, open items and bank matching."""
import hashlib
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.parse import quote_plus
from fastapi import APIRouter, Depends, Form, Request, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy import func, or_
from sqlalchemy.orm import Session
from . import web as w
from ..models.sales import AccountingJournal as Journal, AccountingReconciliation as Reconciliation, AccountingBankTransaction as BankTransaction, AccountingBankMatch as BankMatch
from ..core.accounting_workflow import reconciliation_used, reconcile_lines

router = APIRouter(prefix='/accounting')
KINDS = {'SALES': 'Ventas', 'PURCHASE': 'Compras', 'CASH': 'Caja', 'BANK': 'Banco', 'GENERAL': 'Operaciones diversas'}


def redirect(path, message, error=False):
    return RedirectResponse(f'{path}?{"error" if error else "success"}={quote_plus(message)}', status_code=303)


def money(raw):
    try:
        value = Decimal(str(raw).replace(',', '.'))
        if not value.is_finite() or abs(value) > Decimal('999999999999.99'):
            raise ValueError('Importe inválido.')
        return value.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        raise ValueError('Importe inválido.')


def scoped_journals(db, user):
    branches = w._user_scoped_branch_ids(db, user)
    return db.query(Journal).filter(or_(Journal.branch_id.is_(None), Journal.branch_id.in_(branches)))


def scoped_lines(db, user):
    branches = w._user_scoped_branch_ids(db, user)
    return db.query(w.AccountingEntryLine).join(w.AccountingEntry).filter(
        w.AccountingEntry.estado == 'POSTEADO',
        or_(w.AccountingEntry.branch_id.is_(None), w.AccountingEntry.branch_id.in_(branches)))


def context(request, user, **values):
    return dict(request=request, user=user, version=w.settings.UI_VERSION, today=w.local_today().isoformat(), kinds=KINDS, **values)


@router.get('/journals')
def journals_page(request: Request, db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.financial_data')
    return request.app.state.templates.TemplateResponse('accounting_journals.html', context(request, user,
        journals=scoped_journals(db, user).order_by(Journal.name).all(),
        branches=db.query(w.Branch).filter(w.Branch.id.in_(w._user_scoped_branch_ids(db, user))).all(),
        accounts=db.query(w.CuentaContable).filter_by(activo=True).order_by(w.CuentaContable.codigo).all(),
        voucher_types=db.query(w.AccountingVoucherType).filter_by(activo=True).all()))


@router.post('/journals/save')
def journal_save(request: Request, journal_id: str = Form(''), code: str = Form(...), name: str = Form(...), kind: str = Form(...),
                 voucher_type_id: int = Form(...), currency: str = Form('CS'), branch_id: str = Form(''), main_account_id: str = Form(''),
                 db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.financial_data')
    code = code.strip().upper()
    if not code or len(code) > 20 or not name.strip() or kind not in KINDS or currency not in {'CS', 'USD'}:
        return redirect('/accounting/journals', 'Revisa código, nombre, tipo y moneda.', True)
    if not db.query(w.AccountingVoucherType.id).filter_by(id=voucher_type_id, activo=True).first():
        return redirect('/accounting/journals', 'Tipo de comprobante inválido.', True)
    branch = int(branch_id) if branch_id.isdigit() else None
    if branch_id and (not branch or branch not in w._user_scoped_branch_ids(db, user)):
        return redirect('/accounting/journals', 'Sucursal no permitida.', True)
    account = db.get(w.CuentaContable, int(main_account_id)) if main_account_id.isdigit() else None
    if (main_account_id and (not account or not account.activo)) or (kind in {'BANK', 'CASH'} and (not account or account.tipo != 'BALANCE')):
        return redirect('/accounting/journals', 'Banco y caja requieren una cuenta activa de balance.', True)
    row = scoped_journals(db, user).filter(Journal.id == int(journal_id)).with_for_update().first() if journal_id.isdigit() else None
    if journal_id and not row:
        return redirect('/accounting/journals', 'Diario no encontrado.', True)
    if db.query(Journal.id).filter(Journal.code == code, Journal.id != (row.id if row else -1)).first():
        return redirect('/accounting/journals', 'El código de diario ya existe.', True)
    if row and db.query(BankTransaction.id).filter_by(journal_id=row.id).first() and (row.main_account_id != (account.id if account else None) or row.currency != currency or row.branch_id != branch or row.kind != kind):
        return redirect('/accounting/journals', 'Un diario con extractos conserva cuenta, moneda, tipo y sucursal. Crea otro diario.', True)
    if row is None:
        row = Journal()
        db.add(row)
    row.code, row.name, row.kind, row.currency = code, name.strip()[:120], kind, currency
    row.branch_id, row.main_account_id, row.voucher_type_id, row.active = branch, account.id if account else None, voucher_type_id, True
    db.commit()
    return redirect('/accounting/journals', 'Diario guardado.')


@router.get('/reconcile')
def reconciliation_page(request: Request, account_id: str = '', db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.entries')
    query = scoped_lines(db, user)
    query = query.filter(w.AccountingEntryLine.cuenta_id == int(account_id)) if account_id.isdigit() else query.filter(w.AccountingEntryLine.id == -1)
    rows = []
    for line in query.order_by(w.AccountingEntry.fecha, w.AccountingEntryLine.id).limit(1000).all():
        residual = Decimal(str(line.debe or line.haber)) - reconciliation_used(db, line.id)
        if residual > 0:
            rows.append(dict(line=line, residual=residual, side='Debe' if line.debe > 0 else 'Haber'))
    visible_ids = scoped_lines(db, user).with_entities(w.AccountingEntryLine.id)
    matches = db.query(Reconciliation).filter(or_(Reconciliation.debit_line_id.in_(visible_ids), Reconciliation.credit_line_id.in_(visible_ids))).order_by(Reconciliation.id.desc()).limit(50).all()
    return request.app.state.templates.TemplateResponse('accounting_reconcile.html', context(request, user, rows=rows, matches=matches,
        accounts=db.query(w.CuentaContable).filter_by(activo=True).order_by(w.CuentaContable.codigo).all(), account_id=account_id))


@router.post('/reconcile')
def reconcile(request: Request, debit_line_id: int = Form(...), credit_line_id: int = Form(...), amount: str = Form(...), reason: str = Form(...),
              db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.entries')
    w._lock_accounting_transaction(db)
    lines = scoped_lines(db, user).filter(w.AccountingEntryLine.id.in_([debit_line_id, credit_line_id])).order_by(w.AccountingEntryLine.id).with_for_update().all()
    by_id = {line.id: line for line in lines}
    if len(by_id) != 2 or len(reason.strip()) < 5:
        return redirect('/accounting/reconcile', 'Selecciona dos líneas permitidas y detalla el motivo.', True)
    try:
        reconcile_lines(db, by_id[debit_line_id], by_id[credit_line_id], money(amount), reason.strip(), user.email)
    except ValueError as exc:
        return redirect('/accounting/reconcile', str(exc), True)
    db.commit()
    return redirect('/accounting/reconcile', 'Conciliación aplicada. Los importes restantes siguen pendientes.')


@router.post('/reconcile/{match_id}/undo')
def reconciliation_undo(request: Request, match_id: int, db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.entries')
    w._lock_accounting_transaction(db)
    row = db.query(Reconciliation).filter_by(id=match_id).with_for_update().first()
    if not row or row.canceled_at or scoped_lines(db, user).filter(w.AccountingEntryLine.id.in_([row.debit_line_id, row.credit_line_id])).count() != 2:
        return redirect('/accounting/reconcile', 'Conciliación no disponible.', True)
    row.canceled_at, row.canceled_by = w.local_now_naive(), user.email
    db.commit()
    return redirect('/accounting/reconcile', 'Conciliación deshecha; se conserva su historial.')


def bank_used(db, transaction_id=None, line_id=None):
    query = db.query(func.coalesce(func.sum(BankMatch.amount_cs), 0)).filter(BankMatch.canceled_at.is_(None))
    query = query.filter(BankMatch.transaction_id == transaction_id) if transaction_id else query.filter(BankMatch.line_id == line_id)
    return Decimal(str(query.scalar() or 0))


@router.get('/bank')
def bank_page(request: Request, journal_id: str = '', db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.entries')
    journals = scoped_journals(db, user).filter(Journal.kind.in_(['BANK', 'CASH']), Journal.active.is_(True)).all()
    selected = next((j for j in journals if str(j.id) == journal_id), journals[0] if journals else None)
    transactions, lines, matches = [], [], []
    if selected:
        for item in db.query(BankTransaction).filter_by(journal_id=selected.id).order_by(BankTransaction.fecha.desc(), BankTransaction.id.desc()).limit(500).all():
            transactions.append(dict(item=item, residual=abs(item.amount_cs) - bank_used(db, transaction_id=item.id)))
        if selected.main_account_id:
            query = scoped_lines(db, user).filter(w.AccountingEntryLine.cuenta_id == selected.main_account_id, w.AccountingEntry.branch_id == selected.branch_id)
            for line in query.order_by(w.AccountingEntry.fecha.desc()).limit(500).all():
                residual = abs(line.debe - line.haber) - bank_used(db, line_id=line.id)
                if residual > 0:
                    lines.append(dict(line=line, residual=residual))
        matches = db.query(BankMatch).join(BankTransaction, BankTransaction.id == BankMatch.transaction_id).filter(BankTransaction.journal_id == selected.id).order_by(BankMatch.id.desc()).limit(50).all()
    return request.app.state.templates.TemplateResponse('accounting_bank.html', context(request, user, journals=journals,
        selected=selected, transactions=transactions, lines=lines, matches=matches,
        accounts=db.query(w.CuentaContable).filter_by(activo=True).order_by(w.CuentaContable.codigo).all()))


@router.post('/bank/transaction')
def bank_transaction(request: Request, journal_id: int = Form(...), fecha: str = Form(...), reference: str = Form(...), amount: str = Form(...), rate: str = Form('1'),
                     db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.entries')
    journal = scoped_journals(db, user).filter_by(id=journal_id, active=True).first()
    if not journal or journal.kind not in {'BANK', 'CASH'} or not journal.main_account_id:
        return redirect('/accounting/bank', 'Configura primero la cuenta del diario bancario o de caja.', True)
    try:
        day, value, exchange = date.fromisoformat(fecha), money(amount), Decimal(rate.replace(',', '.'))
        if value == 0 or not reference.strip() or not exchange.is_finite() or exchange <= 0 or exchange > 99999999:
            raise ValueError('Indica fecha, referencia, importe y tasa válidos.')
    except (ValueError, InvalidOperation):
        return redirect('/accounting/bank', 'Indica fecha, referencia, importe y tasa válidos.', True)
    if journal.currency == 'CS':
        exchange = Decimal('1')
    exchange = exchange.quantize(Decimal('.0001'), rounding=ROUND_HALF_UP)
    try:
        converted = money(value * exchange)
        if exchange <= 0 or converted == 0:
            raise ValueError('El importe convertido debe ser distinto de cero.')
    except ValueError as exc:
        return redirect('/accounting/bank', str(exc), True)
    key = hashlib.sha256(f'{day}|{reference.strip()}|{value}|{exchange}'.encode()).hexdigest()
    w._lock_accounting_transaction(db)
    if db.query(BankTransaction.id).filter_by(journal_id=journal_id, import_key=key).first():
        return redirect('/accounting/bank', 'Este movimiento ya fue registrado.', True)
    db.add(BankTransaction(journal_id=journal_id, fecha=day, reference=reference.strip()[:160], amount=value,
        rate=exchange, amount_cs=converted, import_key=key, created_by=user.email))
    db.commit()
    return redirect('/accounting/bank', 'Movimiento de extracto registrado, pendiente de conciliación.')


@router.post('/bank/match')
def bank_match(request: Request, transaction_id: int = Form(...), line_id: int = Form(...), amount_cs: str = Form(...),
               db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.entries')
    w._lock_accounting_transaction(db)
    transaction = db.query(BankTransaction).filter_by(id=transaction_id).with_for_update().first()
    line = scoped_lines(db, user).filter(w.AccountingEntryLine.id == line_id).with_for_update().first()
    if not transaction or not line or not scoped_journals(db, user).filter(Journal.id == transaction.journal_id).first():
        return redirect('/accounting/bank', 'Movimiento o asiento no permitido.', True)
    journal = transaction.journal
    signed = line.debe - line.haber
    if journal.main_account_id != line.cuenta_id or journal.branch_id != line.entry.branch_id or (signed > 0) != (transaction.amount_cs > 0):
        return redirect('/accounting/bank', 'Deben coincidir cuenta, sucursal y sentido del movimiento.', True)
    try:
        value = money(amount_cs)
        if value <= 0 or value > abs(signed) - bank_used(db, line_id=line.id) or value > abs(transaction.amount_cs) - bank_used(db, transaction_id=transaction.id):
            raise ValueError('El importe supera el saldo disponible o no es positivo.')
    except ValueError as exc:
        return redirect('/accounting/bank', str(exc), True)
    db.add(BankMatch(transaction_id=transaction.id, line_id=line.id, amount_cs=value, created_by=user.email))
    db.commit()
    return redirect('/accounting/bank', 'Movimiento conciliado sin duplicar su asiento contable.')


@router.post('/bank/match/{match_id}/undo')
def bank_undo(request: Request, match_id: int, db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request, user, 'access.accounting.entries')
    w._lock_accounting_transaction(db)
    row = db.query(BankMatch).filter_by(id=match_id).with_for_update().first()
    transaction = db.get(BankTransaction, row.transaction_id) if row else None
    if not row or row.canceled_at or not scoped_journals(db, user).filter(Journal.id == transaction.journal_id).first():
        return redirect('/accounting/bank', 'Conciliación no disponible.', True)
    row.canceled_at, row.canceled_by = w.local_now_naive(), user.email
    db.commit()
    return redirect('/accounting/bank', 'Conciliación bancaria deshecha; se conserva su historial.')


@router.post('/bank/counterpart')
def bank_counterpart(request: Request, transaction_id: int = Form(...), account_id: int = Form(...), description: str = Form(...),
                     db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    """Record an unbooked bank fee/receipt and match it in one transaction."""
    w._enforce_permission(request, user, 'access.accounting.entries')
    w._lock_accounting_transaction(db)
    transaction = db.query(BankTransaction).filter_by(id=transaction_id).with_for_update().first()
    if not transaction or not scoped_journals(db, user).filter(Journal.id == transaction.journal_id, Journal.active.is_(True)).first():
        return redirect('/accounting/bank', 'Movimiento no permitido.', True)
    journal = transaction.journal
    account = db.get(w.CuentaContable, account_id)
    if not journal.main_account_id or not account or not account.activo or account.id == journal.main_account_id or len(description.strip()) < 5:
        return redirect('/accounting/bank', 'Selecciona una contrapartida activa distinta del banco y detalla el concepto.', True)
    if db.query(w.AccountingEntry.id).filter(w.AccountingEntry.referencia == f'EXTRACTO-{transaction.id}', w.AccountingEntry.estado == 'POSTEADO').first():
        return redirect('/accounting/bank', 'Ya existe un asiento para este movimiento. Vuelve a vincularlo en lugar de crear otra contrapartida.', True)
    residual = abs(transaction.amount_cs) - bank_used(db, transaction_id=transaction.id)
    if residual <= 0:
        return redirect('/accounting/bank', 'El movimiento ya está conciliado.', True)
    try:
        w._assert_accounting_date_open(db, transaction.fecha)
    except HTTPException as exc:
        return redirect('/accounting/bank', str(exc.detail), True)
    period = w._accounting_period(transaction.fecha)
    sequence = w._next_accounting_sequence(db, journal.voucher_type_id, journal.branch_id, period)
    incoming = transaction.amount_cs > 0
    bank_line = w.AccountingEntryLine(cuenta_id=journal.main_account_id, descripcion=description.strip()[:200],
        debe=residual if incoming else 0, haber=0 if incoming else residual)
    counterpart = w.AccountingEntryLine(cuenta_id=account.id, descripcion=description.strip()[:200],
        debe=0 if incoming else residual, haber=residual if incoming else 0)
    entry = w.AccountingEntry(tipo_id=journal.voucher_type_id, journal_id=journal.id, branch_id=journal.branch_id,
        fecha=transaction.fecha, periodo=period, secuencia=sequence,
        numero=w._build_accounting_entry_number(journal.voucher_type, period, sequence),
        referencia=f'EXTRACTO-{transaction.id}', descripcion=description.strip(), moneda='CS', estado='POSTEADO',
        total_debe=residual, total_haber=residual, creado_por=user.email,
        publicado_por=user.email, publicado_at=w.local_now_naive(), lines=[bank_line, counterpart])
    db.add(entry)
    db.flush()
    db.add(BankMatch(transaction_id=transaction.id, line_id=bank_line.id, amount_cs=residual, created_by=user.email))
    db.commit()
    return redirect('/accounting/bank', f'Contrapartida {entry.numero} publicada y conciliada.')


@router.get('/periods')
def periods_page(request: Request, db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    from datetime import timedelta
    from ..models.sales import AccountingPeriodClosure, AccountingPeriodEvent
    w._enforce_permission(request, user, 'access.accounting')
    last_month = w.local_today().replace(day=1) - timedelta(days=1)
    periods = db.query(AccountingPeriodClosure).order_by(AccountingPeriodClosure.end_date.desc(), AccountingPeriodClosure.kind).all()
    effective = {}
    for period in periods:
        effective[period.id] = [other.period for other in periods if other.id != period.id and other.closed and other.start_date <= period.end_date and other.end_date >= period.start_date]
    return request.app.state.templates.TemplateResponse('accounting_periods.html', context(request, user, periods=periods, blockers=effective,
        events=db.query(AccountingPeriodEvent).order_by(AccountingPeriodEvent.id.desc()).limit(100).all(),
        last_month=last_month.strftime('%Y-%m'), last_year=w.local_today().year-1,
        can_manage=any(role.name=='administrador' for role in user.roles)))


def period_admin(request, user):
    w._enforce_permission(request, user, 'access.accounting.financial_data')
    if not any(role.name=='administrador' for role in user.roles):
        raise HTTPException(status_code=403, detail='Solo el administrador puede cerrar o reabrir períodos contables.')


@router.post('/periods/close')
def period_close(request: Request, kind: str = Form(...), period: str = Form(...), reason: str = Form(...), db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    from ..core.accounting_periods import close_period
    period_admin(request, user)
    w._lock_accounting_transaction(db)
    try:
        row = close_period(db, kind, period, reason, user.email, w.local_today())
    except ValueError as exc:
        return redirect('/accounting/periods', str(exc), True)
    db.commit()
    return redirect('/accounting/periods', f'Período {row.period} cerrado. Se bloquean los registros dentro de sus fechas.')


@router.post('/periods/{closure_id}/reopen')
def period_reopen(request: Request, closure_id: int, reason: str = Form(...), db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    from ..core.accounting_periods import reopen_period
    period_admin(request, user)
    w._lock_accounting_transaction(db)
    try:
        row, blockers = reopen_period(db, closure_id, reason, user.email)
    except ValueError as exc:
        return redirect('/accounting/periods', str(exc), True)
    db.commit()
    message = f'Período {row.period} reabierto. Ya puedes corregir y volver a cerrarlo.'
    if blockers:
        message = f'Período {row.period} reabierto. Todavía hay fechas bloqueadas por otros cierres vigentes: ' + ', '.join(p.period for p in blockers)
    return redirect('/accounting/periods', message)


@router.post('/periods/{closure_id}/close')
def period_reclose(request: Request, closure_id: int, reason: str = Form(...), db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    from ..core.accounting_periods import reclose_period
    period_admin(request, user)
    w._lock_accounting_transaction(db)
    try:
        row = reclose_period(db, closure_id, reason, user.email, w.local_today())
    except ValueError as exc:
        return redirect('/accounting/periods', str(exc), True)
    db.commit()
    return redirect('/accounting/periods', f'Período {row.period} cerrado nuevamente.')


@router.get('/accounts/options')
def account_options(request: Request, db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    from fastapi.responses import JSONResponse
    from ..core.accounting_catalog import account_paths
    w._enforce_permission(request,user,'access.accounting.entries')
    accounts = db.query(w.CuentaContable).order_by(w.CuentaContable.codigo).all()
    paths = account_paths(accounts)
    return JSONResponse({'accounts':[dict(id=account.id,codigo=account.codigo,nombre=account.nombre,
        descripcion=account.descripcion or '',path=paths[account.id],tipo=account.tipo,naturaleza=account.naturaleza) for account in accounts if account.activo]})


@router.get('/accounts')
def accounts_page(request: Request, db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    from ..core.accounting_catalog import descendants, account_paths, chart_rows, suggested_code
    w._enforce_permission(request,user,'access.accounting.financial_data')
    accounts = db.query(w.CuentaContable).order_by(w.CuentaContable.codigo).all()
    by_id = {account.id:account for account in accounts}
    edit_id = request.query_params.get('edit_id','')
    item = by_id.get(int(edit_id)) if edit_id.isdigit() else None
    if edit_id and not item:
        return redirect('/accounting/accounts','Cuenta no encontrada.',True)
    parent_id = request.query_params.get('parent_id','')
    parent = by_id.get(item.parent_id) if item else by_id.get(int(parent_id)) if parent_id.isdigit() else None
    excluded = descendants(accounts,item.id)|{item.id} if item else set()
    paths = account_paths(accounts)
    return request.app.state.templates.TemplateResponse('accounting_accounts.html',context(request,user,
        rows=chart_rows(accounts),accounts=accounts,edit_item=item,parent=parent,paths=paths,
        parent_accounts=[account for account in accounts if account.id not in excluded and (account.activo or (parent and account.id==parent.id)) and (account.nivel or 1)<4],
        suggested=suggested_code(parent,accounts),
        parents_json=[dict(id=account.id,path=paths[account.id],tipo=account.tipo,naturaleza=account.naturaleza,nivel=account.nivel,code=suggested_code(account,accounts)) for account in accounts]))


@router.post('/accounts/save')
def account_save(request: Request, account_id: str = Form(''), codigo: str = Form(...), nombre: str = Form(...), descripcion: str = Form(''),
                 tipo: str = Form(...), naturaleza: str = Form(...), parent_id: str = Form(''), activo: str = Form(''), db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    from ..core.accounting_catalog import save_account
    w._enforce_permission(request,user,'access.accounting.financial_data')
    if (account_id and not account_id.isdigit()) or (parent_id and not parent_id.isdigit()):
        return redirect('/accounting/accounts','Cuenta o ubicación inválida.',True)
    w._lock_accounting_transaction(db)
    try:
        item=save_account(db,account_id=int(account_id) if account_id else None,codigo=codigo,nombre=nombre,descripcion=descripcion,
            tipo=tipo,naturaleza=naturaleza,parent_id=int(parent_id) if parent_id else None,active=activo=='on')
    except ValueError as exc:
        return redirect('/accounting/accounts',str(exc),True)
    db.commit()
    return redirect('/accounting/accounts',f'Cuenta {item.codigo} guardada en el catálogo.')


@router.get('/voucher-types')
def voucher_types_page(request: Request, db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    w._enforce_permission(request,user,'access.accounting.voucher_types')
    return request.app.state.templates.TemplateResponse('accounting_voucher_types.html',context(request,user,
        types=db.query(w.AccountingVoucherType).order_by(w.AccountingVoucherType.code).all()))


@router.post('/voucher-types/save')
def voucher_type_save(request: Request, item_id: str = Form(''), code: str = Form(...), nombre: str = Form(...), prefijo: str = Form(...), activo: str = Form(''),
                      db: Session = Depends(w.get_db), user=Depends(w._require_admin_web)):
    import re
    w._enforce_permission(request,user,'access.accounting.voucher_types')
    w._lock_accounting_transaction(db)
    code,prefijo,nombre=code.strip().upper(),prefijo.strip().upper(),nombre.strip()
    if not re.fullmatch(r'[A-Z0-9_]{1,20}',code) or not re.fullmatch(r'[A-Z0-9_-]{1,10}',prefijo) or not nombre or len(nombre)>120:
        return redirect('/accounting/voucher-types','Revisa código, nombre y prefijo del comprobante.',True)
    row=db.get(w.AccountingVoucherType,int(item_id)) if item_id.isdigit() else None
    if item_id and not row:
        return redirect('/accounting/voucher-types','Tipo no encontrado.',True)
    if row and code!=row.code:
        return redirect('/accounting/voucher-types','El código de un tipo existente se conserva para sus integraciones.',True)
    if db.query(w.AccountingVoucherType.id).filter(w.AccountingVoucherType.id!=(row.id if row else -1),or_(func.upper(w.AccountingVoucherType.code)==code,func.upper(w.AccountingVoucherType.prefijo)==prefijo)).first():
        return redirect('/accounting/voucher-types','El código o prefijo ya pertenece a otro tipo.',True)
    if not row:
        row=w.AccountingVoucherType(code=code);db.add(row)
    row.nombre,row.prefijo,row.activo=nombre,prefijo,activo=='on'
    db.commit()
    return redirect('/accounting/voucher-types','Tipo de comprobante guardado.')
