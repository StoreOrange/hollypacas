"""Accounting invariants shared by entry posting and reversal."""
from decimal import Decimal

POSTED_STATES = ("POSTEADO", "REVERSADO")


def validate_postable_entry(entry, *, allow_inactive=False):
    if len(entry.lines) < 2:
        raise ValueError("El asiento requiere al menos dos líneas con monto.")
    debit = Decimal("0")
    credit = Decimal("0")
    for line in entry.lines:
        if not line.cuenta or (not allow_inactive and not line.cuenta.activo):
            raise ValueError("Todas las cuentas del asiento deben estar activas.")
        d, c = Decimal(str(line.debe or 0)), Decimal(str(line.haber or 0))
        if not d.is_finite() or not c.is_finite() or d < 0 or c < 0:
            raise ValueError("Los montos deben ser válidos y no negativos.")
        if (d > 0) == (c > 0):
            raise ValueError("Cada línea debe tener un monto en Debe o Haber.")
        debit += d
        credit += c
    if debit <= 0 or debit != credit:
        raise ValueError("El asiento no cuadra: Debe y Haber deben ser iguales.")
    if debit != Decimal(str(entry.total_debe)) or credit != Decimal(str(entry.total_haber)):
        raise ValueError("Los totales del asiento no coinciden con sus líneas.")
    return debit, credit


def journal_for_reference(db, reference):
    from ..models.sales import AccountingJournal
    kind = "SALES" if str(reference or "").startswith("AUTO-VTA-") else "GENERAL"
    journal = db.query(AccountingJournal).filter_by(kind=kind, active=True, branch_id=None).order_by(AccountingJournal.id).first()
    return journal.id if journal else None


def seed_journals(db):
    from ..models.sales import AccountingJournal, AccountingVoucherType, AccountingEntry
    types = {v.code: v.id for v in db.query(AccountingVoucherType).all()}
    defaults = [("VEN", "Ventas", "SALES", "INGRESO"), ("COM", "Compras", "PURCHASE", "EGRESO"),
                ("CAJA", "Caja", "CASH", "DIARIO"), ("BANCO", "Banco", "BANK", "DIARIO"),
                ("VAR", "Operaciones diversas", "GENERAL", "DIARIO")]
    for code, name, kind, voucher in defaults:
        if not db.query(AccountingJournal.id).filter_by(code=code).first() and voucher in types:
            db.add(AccountingJournal(code=code, name=name, kind=kind, voucher_type_id=types[voucher], currency="CS", active=True))
    db.flush()
    # El diario estándar usa la misma Caja 1101 de la vista previa ROC existente.
    # Solo completar la configuración ausente; nunca cambiar una cuenta elegida.
    from ..models.sales import CuentaContable
    cash = db.query(CuentaContable).filter_by(codigo="1101", activo=True, tipo="BALANCE", naturaleza="DEBE").first()
    journal = db.query(AccountingJournal).filter_by(code="CAJA", kind="CASH", branch_id=None).first()
    if cash and journal and not journal.main_account_id and "caja" in cash.nombre.lower():
        journal.main_account_id = cash.id
    db.flush()
    sales_id = journal_for_reference(db, "AUTO-VTA-0")
    general_id = journal_for_reference(db, "")
    if sales_id:
        db.query(AccountingEntry).filter(AccountingEntry.journal_id.is_(None), AccountingEntry.referencia.like("AUTO-VTA-%")).update({AccountingEntry.journal_id: sales_id}, synchronize_session=False)
    if general_id:
        db.query(AccountingEntry).filter(AccountingEntry.journal_id.is_(None)).update({AccountingEntry.journal_id: general_id}, synchronize_session=False)
    # Recover contact links only from an exact invoice reference. Never infer
    # partners from descriptions or account names.
    from ..models.sales import VentaFactura, AccountingEntryLine
    sourced = db.query(AccountingEntry).filter(AccountingEntry.referencia.like("AUTO-VTA-%")).all()
    ids = {int(entry.referencia[9:]) for entry in sourced if entry.referencia[9:].isdigit()}
    invoices = {invoice.id: invoice.cliente_id for invoice in db.query(VentaFactura).filter(VentaFactura.id.in_(ids)).all()}
    for entry in sourced:
        suffix = entry.referencia[9:]
        client_id = invoices.get(int(suffix)) if suffix.isdigit() else None
        if client_id:
            db.query(AccountingEntryLine).filter(AccountingEntryLine.entry_id == entry.id, AccountingEntryLine.cliente_id.is_(None), AccountingEntryLine.proveedor_id.is_(None)).update({AccountingEntryLine.cliente_id: client_id}, synchronize_session=False)
    db.commit()


def reconciliation_used(db, line_id):
    from sqlalchemy import func, or_
    from ..models.sales import AccountingReconciliation
    value = db.query(func.coalesce(func.sum(AccountingReconciliation.amount_cs), 0)).filter(
        AccountingReconciliation.canceled_at.is_(None),
        or_(AccountingReconciliation.debit_line_id == line_id, AccountingReconciliation.credit_line_id == line_id)).scalar()
    return Decimal(str(value or 0))


def reconcile_lines(db, debit, credit, amount, reason, actor):
    from ..models.sales import AccountingReconciliation
    if debit.id == credit.id or debit.entry_id == credit.entry_id:
        raise ValueError("Selecciona líneas de dos asientos distintos.")
    if debit.entry.estado != "POSTEADO" or credit.entry.estado != "POSTEADO":
        raise ValueError("Solo se concilian asientos publicados y sin reversión.")
    if debit.cuenta_id != credit.cuenta_id or debit.entry.branch_id != credit.entry.branch_id:
        raise ValueError("Las líneas deben pertenecer a la misma cuenta y sucursal.")
    if (debit.cliente_id, debit.proveedor_id) != (credit.cliente_id, credit.proveedor_id):
        raise ValueError("Las líneas deben pertenecer al mismo cliente o proveedor.")
    amount = Decimal(str(amount))
    if not amount.is_finite() or amount <= 0 or amount != amount.quantize(Decimal("0.01")):
        raise ValueError("Indica un importe positivo con hasta dos decimales.")
    if debit.debe <= 0 or credit.haber <= 0:
        raise ValueError("Selecciona un cargo en Debe y su abono en Haber.")
    if amount > debit.debe - reconciliation_used(db, debit.id) or amount > credit.haber - reconciliation_used(db, credit.id):
        raise ValueError("El importe supera el saldo pendiente de alguna línea.")
    row = AccountingReconciliation(debit_line_id=debit.id, credit_line_id=credit.id, amount_cs=amount,
                                  reason=reason[:260], created_by=actor)
    db.add(row)
    db.flush()
    return row
