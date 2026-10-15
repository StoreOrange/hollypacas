"""Un recibo y un asiento; el asiento nunca vuelve a tocar caja operativa."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from sqlalchemy import or_
from ..models.sales import AccountingJournal, AccountingEntry, AccountingEntryLine, CuentaContable


def receipt_amounts(value, currency, rate):
    raw = str(value).strip()
    if ',' in raw and not re.fullmatch(r'\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?', raw):
        raise ValueError('Formato inválido: usa 1,234.56 o 1234.56')
    try:
        amount = Decimal(raw.replace(',', ''))
        exchange = Decimal(str(rate or 0))
    except InvalidOperation:
        raise ValueError('Monto o tasa inválida')
    if not amount.is_finite() or amount <= 0 or amount > Decimal('9999999999.99') or amount != amount.quantize(Decimal('.01')):
        raise ValueError('Escribe un monto positivo, con máximo dos decimales')
    if currency not in {'CS', 'USD'}:
        raise ValueError('Moneda inválida')
    if not exchange.is_finite() or exchange < 0 or (currency == 'USD' and exchange <= 0):
        raise ValueError('Se requiere una tasa de cambio válida')
    rounded = lambda number: number.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
    return (amount, rounded(amount * exchange)) if currency == 'USD' else (rounded(amount / exchange) if exchange else Decimal(0), amount)


def available_cash_journals(db, branch_id):
    return db.query(AccountingJournal).filter(AccountingJournal.active.is_(True), AccountingJournal.kind == 'CASH',
        or_(AccountingJournal.branch_id == branch_id, AccountingJournal.branch_id.is_(None))).order_by(AccountingJournal.name).all()


def resolve_cash_journal(db, branch_id, selected_id=None):
    journals = available_cash_journals(db, branch_id)
    if selected_id:
        journal = next((row for row in journals if row.id == int(selected_id)), None)
        if not journal:
            raise ValueError('Selecciona un diario de caja habilitado para la sucursal')
    else:
        scoped = [row for row in journals if row.branch_id == branch_id]
        choices = scoped or [row for row in journals if row.branch_id is None]
        if len(choices) != 1:
            raise ValueError('Selecciona el diario de caja para contabilizar el recibo')
        journal = choices[0]
    account = db.get(CuentaContable, journal.main_account_id) if journal.main_account_id else None
    if not account or not account.activo or account.tipo != 'BALANCE' or account.naturaleza != 'DEBE':
        raise ValueError('Configura una cuenta de caja activa en el diario seleccionado')
    return journal, account


def build_receipt_entry(db, receipt, rubro, journal_id, actor, counter_account_id=None):
    from ..routers import web
    reference = f'AUTO-ROC-{receipt.id}'
    existing = db.query(AccountingEntry).filter(AccountingEntry.referencia == reference).first()
    if existing:
        return existing
    journal, cash = resolve_cash_journal(db, receipt.branch_id, journal_id)
    counter_id = rubro.cuenta_id or (int(counter_account_id) if counter_account_id else None)
    counter = db.get(CuentaContable, counter_id) if counter_id else None
    if not counter or not counter.activo or counter.id == cash.id:
        raise ValueError('El rubro requiere una cuenta contable activa, distinta de caja. Configura el rubro antes de registrar.')
    web._assert_accounting_date_open(db, receipt.fecha)
    voucher = web._find_voucher_type_for_code(db, receipt.tipo)
    if not voucher or not voucher.activo:
        raise ValueError('Habilita el tipo de comprobante ' + receipt.tipo)
    amount = Decimal(str(receipt.monto_cs)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
    if amount <= 0:
        raise ValueError('El equivalente en C$ debe ser mayor que cero')
    debit, credit = (cash, counter) if receipt.tipo == 'INGRESO' else (counter, cash)
    period = web._accounting_period(receipt.fecha)
    sequence = web._next_accounting_sequence(db, voucher.id, receipt.branch_id, period)
    entry = AccountingEntry(tipo_id=voucher.id, journal_id=journal.id, branch_id=receipt.branch_id,
        fecha=receipt.fecha, periodo=period, secuencia=sequence,
        numero=web._build_accounting_entry_number(voucher, period, sequence), referencia=reference,
        descripcion=f'Recibo {receipt.numero}: {receipt.descripcion or rubro.nombre}'[:400], estado='POSTEADO',
        total_debe=amount, total_haber=amount, creado_por=actor, publicado_por=actor, publicado_at=web.local_now_naive(),
        lines=[AccountingEntryLine(cuenta=debit, descripcion=f'{receipt.numero} · {rubro.nombre}'[:200], debe=amount, haber=0),
               AccountingEntryLine(cuenta=credit, descripcion=f'{receipt.numero} · Caja'[:200], debe=0, haber=amount)])
    web.validate_postable_entry(entry)
    db.add(entry); db.flush()
    return entry
