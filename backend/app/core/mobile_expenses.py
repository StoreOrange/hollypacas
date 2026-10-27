"""Assisted expenses: server-owned categories, one posting and cash movement at most once."""
from datetime import date
from decimal import Decimal
import re
from sqlalchemy import or_, func
from ..models.sales import (MobileExpense, AccountingEntry, AccountingEntryLine, AccountingJournal,
                            CuentaContable, ReciboRubro, ReciboMotivo, ReciboCaja, CajaDiaria, CierreCaja)
from ..models.inventory import Bodega, ExchangeRate
from .accounting_cash_receipts import receipt_amounts, build_receipt_entry, resolve_cash_journal

PERMISSION = 'access.accounting.mobile_expenses'
CATEGORIES = [
    ('extraordinary', 'Gasto extraordinario / imprevisto', '6901', 'Describe qué ocurrió y por qué fue necesario.'),
    ('transport', 'Transporte y fletes', '6104', 'Pasajes, transporte de trabajo y fletes.'),
    ('fuel', 'Combustible', '6103', 'Combustible utilizado en actividades del negocio.'),
    ('repair', 'Mantenimiento y reparaciones', '6105', 'Reparaciones y mantenimiento; las compras de equipos se registran aparte.'),
    ('cleaning', 'Limpieza', '6107', 'Artículos y servicios de limpieza del negocio.'),
    ('supplies', 'Insumos del negocio', '6108', 'Materiales de uso operativo; no mercadería para vender.'),
    ('electricity', 'Energía eléctrica', '6110', 'Pago del servicio de energía.'),
    ('water', 'Agua potable', '6111', 'Pago del servicio de agua.'),
    ('internet', 'Teléfono e internet', '6112', 'Telefonía e internet del negocio.'),
    ('stationery', 'Papelería y útiles', '6203', 'Útiles y materiales de oficina.'),
    ('rent', 'Alquiler', '6204', 'Alquiler del período actual; anticipos se revisan aparte.'),
    ('professional', 'Servicios profesionales', '6205', 'Honorarios y servicios profesionales prestados.'),
    ('security', 'Seguridad y vigilancia', '6106', 'Servicios de seguridad del negocio.'),
    ('advertising', 'Publicidad', '6403', 'Publicidad y promoción del negocio.'),
    ('bank_fees', 'Comisión bancaria', '6304', 'Comisiones cobradas por el banco.'),
    ('other', 'Otro gasto del negocio', '6901', 'Explica el destino y conserva el soporte del gasto.'),
]
CATEGORY_MAP = {row[0]: row for row in CATEGORIES}


def expense_account(db, category):
    spec = CATEGORY_MAP.get(category)
    if not spec:
        raise ValueError('Selecciona el tipo de gasto.')
    account = db.query(CuentaContable).filter_by(codigo=spec[2], activo=True, tipo='RESULTADO', naturaleza='DEBE').first()
    if not account or db.query(CuentaContable.id).filter_by(parent_id=account.id).first():
        raise ValueError('La cuenta de este tipo de gasto necesita revisión del contador. Elige otro tipo o solicita su configuración.')
    return account


def payment_choices(db, branch_ids):
    choices = []
    journals = db.query(AccountingJournal).filter(AccountingJournal.active.is_(True),
        AccountingJournal.kind.in_(['CASH','BANK']),
        or_(AccountingJournal.branch_id.is_(None),AccountingJournal.branch_id.in_(branch_ids))).order_by(AccountingJournal.name).all()
    for journal in journals:
        account = db.get(CuentaContable,journal.main_account_id) if journal.main_account_id else None
        if not account or not account.activo or account.tipo!='BALANCE' or account.naturaleza!='DEBE':continue
        # Use the explicitly configured cash/bank account, as the ROC flow does.
        # A configured 1101/1102 account can legitimately have subaccounts.
        kind = 'cash' if journal.kind=='CASH' else 'bank'
        if not account.codigo.startswith('1101' if kind=='cash' else '1102'):continue
        choices.append({'value':f'{kind}:{journal.id}','label':('Caja del negocio' if kind=='cash' else 'Banco del negocio')+' · '+journal.name,
                        'branch_id':journal.branch_id,'kind':kind})
    payable = db.query(CuentaContable).filter_by(codigo='2109',activo=True,tipo='BALANCE',naturaleza='HABER').first()
    if payable and not db.query(CuentaContable.id).filter_by(parent_id=payable.id).first():
        choices.append({'value':'personal','label':'Lo pagó propietario / gerencia (por reembolsar)','branch_id':None,'kind':'personal'})
    return choices


def _text(data,key,label,minimum=0,maximum=160):
    value=str(data.get(key) or '').strip()
    if len(value)<minimum or len(value)>maximum:
        raise ValueError(f'{label}: escribe entre {minimum} y {maximum} caracteres.')
    return value


def register_expense(db,user,data):
    from ..routers import web as w
    w._lock_accounting_transaction(db)
    key=str(data.get('operation_key') or '')
    if not re.fullmatch('[a-f0-9]{32}',key):raise ValueError('Recarga el formulario para registrar el gasto.')
    scopes=w._user_scoped_branch_ids(db,user)
    existing=db.query(MobileExpense).filter_by(operation_key=key).first()
    if existing:
        if existing.user_id!=user.id or existing.branch_id not in scopes:
            raise ValueError('Esta solicitud ya pertenece a otro registro.')
        return existing
    raw_branch=str(data.get('branch_id') or '')
    if not raw_branch.isdigit() or int(raw_branch) not in scopes:raise ValueError('Selecciona una sucursal autorizada.')
    branch_id=int(raw_branch)
    try: day=date.fromisoformat(str(data.get('fecha') or ''))
    except ValueError:raise ValueError('Indica una fecha válida.')
    if day>w.local_today():raise ValueError('La fecha del gasto no puede ser futura.')
    category=str(data.get('category') or '')
    account=expense_account(db,category)
    detail=_text(data,'descripcion','Motivo del gasto',5,400)
    beneficiary=_text(data,'beneficiary','Quién recibió el pago',2)
    reference=_text(data,'reference','Factura / referencia')
    if not reference and data.get('no_support')!='on':raise ValueError('Indica la factura o marca que el soporte está pendiente.')
    payment=str(data.get('payment') or '')
    choice=next((c for c in payment_choices(db,{branch_id}) if c['value']==payment),None)
    if not choice:raise ValueError('Selecciona de dónde salió el dinero.')
    paid_by=_text(data,'paid_by','Quién puso el dinero',2) if choice['kind']=='personal' else None
    currency=str(data.get('moneda') or '').upper()
    rate_row=db.query(ExchangeRate).filter(ExchangeRate.effective_date<=day).order_by(ExchangeRate.effective_date.desc(),ExchangeRate.id.desc()).first()
    rate=Decimal(str(rate_row.rate if rate_row else 0))
    usd,cs=receipt_amounts(data.get('monto',''),currency,rate)
    if cs<=0:raise ValueError('El equivalente del gasto en córdobas debe ser mayor que cero.')
    if max(cs,usd)>Decimal('999999999999.99'):raise ValueError('El monto convertido excede el límite permitido.')
    w._assert_accounting_date_open(db,day)
    warehouse=None;receipt=None
    actor=user.full_name or user.email
    if choice['kind']=='cash':
        raw_bodega=str(data.get('bodega_id') or '')
        if not raw_bodega.isdigit():raise ValueError('Selecciona la caja / bodega que pagó el gasto.')
        warehouse=db.query(Bodega).filter_by(id=int(raw_bodega),branch_id=branch_id,activo=True).first()
        if not warehouse:raise ValueError('La bodega no pertenece a la sucursal seleccionada.')
        if db.query(CierreCaja.id).filter_by(bodega_id=warehouse.id,fecha=day).first():raise ValueError('La caja de esa fecha ya está cerrada.')
        journal_id=int(payment.split(':')[1])
        resolve_cash_journal(db,branch_id,journal_id)
        if db.get_bind().dialect.name=='postgresql':db.execute(w.sql_text('SELECT pg_advisory_xact_lock(83742020)'))
        rubro_name='Gasto móvil · '+CATEGORY_MAP[category][1]
        rubro=db.query(ReciboRubro).filter_by(nombre=rubro_name).first()
        if not rubro:
            rubro=ReciboRubro(nombre=rubro_name,tipo='EGRESO',cuenta_id=account.id,activo=True);db.add(rubro);db.flush()
        if not rubro.activo or rubro.tipo!='EGRESO' or rubro.cuenta_id!=account.id:raise ValueError('El rubro de gasto necesita revisión contable.')
        motive=db.query(ReciboMotivo).filter_by(nombre='Gasto móvil autorizado').first()
        if not motive:
            motive=ReciboMotivo(nombre='Gasto móvil autorizado',tipo='EGRESO',activo=True);db.add(motive);db.flush()
        if not motive.activo or motive.tipo!='EGRESO':raise ValueError('El motivo de gasto necesita revisión.')
        branch=db.get(w.Branch,branch_id)
        code=(branch.code or '').lower()
        prefix='ROC-C' if code=='central' else 'ROC-E' if code=='esteli' else 'ROC-'+code[:1].upper()
        seq=int(db.query(func.max(ReciboCaja.secuencia)).filter(ReciboCaja.numero.like(prefix+'-%')).scalar() or 0)+1
        receipt=ReciboCaja(operation_key=key,numero=f'{prefix}-{seq:05d}',secuencia=seq,branch_id=branch_id,bodega_id=warehouse.id,
            tipo='EGRESO',rubro_id=rubro.id,motivo_id=motive.id,descripcion=f'{beneficiary}: {detail}'[:400],fecha=day,
            moneda=currency,tasa_cambio=rate if currency=='USD' else None,monto_usd=usd,monto_cs=cs,afecta_caja=True,usuario_registro=actor[:120])
        db.add(receipt);db.flush()
        entry=build_receipt_entry(db,receipt,rubro,journal_id,actor)
        entry.manual_operation_key=key
        receipt.accounting_entry_id=entry.id
        cash=db.query(CajaDiaria).filter_by(branch_id=branch_id,bodega_id=warehouse.id,fecha=day).with_for_update().first()
        if not cash:
            cash=CajaDiaria(branch_id=branch_id,bodega_id=warehouse.id,fecha=day,saldo_cs=0,saldo_usd=0);db.add(cash)
        cash.saldo_cs=Decimal(str(cash.saldo_cs or 0))-cs
        cash.saldo_usd=Decimal(str(cash.saldo_usd or 0))-usd
    else:
        if choice['kind']=='bank':
            journal=db.get(AccountingJournal,int(payment.split(':')[1]));credit=db.get(CuentaContable,journal.main_account_id)
        else:
            credit=db.query(CuentaContable).filter_by(codigo='2109',activo=True).one()
            journals=db.query(AccountingJournal).filter_by(active=True,kind='GENERAL').filter(or_(AccountingJournal.branch_id==branch_id,AccountingJournal.branch_id.is_(None))).order_by(AccountingJournal.id).all()
            if not journals:raise ValueError('Configura el diario de operaciones generales.')
            journal=next((j for j in journals if j.branch_id==branch_id),journals[0])
        voucher=w._find_voucher_type_for_code(db,'EGRESO')
        if not voucher or not voucher.activo:raise ValueError('Habilita el comprobante de egreso.')
        period=w._accounting_period(day);seq=w._next_accounting_sequence(db,voucher.id,branch_id,period)
        entry=AccountingEntry(tipo_id=voucher.id,journal_id=journal.id,branch_id=branch_id,fecha=day,periodo=period,secuencia=seq,
            numero=w._build_accounting_entry_number(voucher,period,seq),referencia=reference or 'Gasto móvil sin soporte',
            descripcion=detail,estado='POSTEADO',total_debe=cs,total_haber=cs,manual_operation_key=key,
            creado_por=user.email,publicado_por=user.email,publicado_at=w.local_now_naive(),
            lines=[AccountingEntryLine(cuenta=account,descripcion=detail[:200],debe=cs,haber=0),
                   AccountingEntryLine(cuenta=credit,descripcion=(f'Por reembolsar a {paid_by}' if paid_by else f'Pago a {beneficiary}')[:200],debe=0,haber=cs)])
        db.add(entry);db.flush()
    entry.moneda=currency;entry.tasa_cambio=rate if currency=='USD' else None
    entry.tasa_sistema=rate;entry.creado_por=user.email;entry.publicado_por=user.email
    if receipt:
        entry.descripcion=detail+'\nSoporte: '+(reference or 'Pendiente')
        entry.lines[0].descripcion=(detail+' · '+beneficiary)[:200]
    w.validate_postable_entry(entry)
    expense=MobileExpense(operation_key=key,entry_id=entry.id,cash_receipt_id=receipt.id if receipt else None,
        branch_id=branch_id,bodega_id=warehouse.id if warehouse else None,category=category,payment=choice['kind'],
        beneficiary=beneficiary,paid_by=paid_by,support_reference=reference or None,user_id=user.id,
        amount=usd if currency=='USD' else cs,currency=currency,exchange_rate=rate or None)
    db.add(expense);db.flush()
    return expense
