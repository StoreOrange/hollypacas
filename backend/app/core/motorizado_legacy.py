"""Reclasificación explícita de registros de la primera versión local. No se ejecuta al iniciar la app."""
from .motorizado import expense_entry
from ..models.motorizado import MotoTrip,MotoTripEvent


def isolate_legacy(db,trip,actor):
    """Reclasificación explícita de la primera versión local, sin borrar sus recibos."""
    from decimal import Decimal
    from ..routers import web
    from ..models.sales import CajaDiaria
    if not web._is_pacasholl_company():raise ValueError('Reclasificación exclusiva de Pacas Holl')
    if trip.accounting_entry_id:return
    actor_name=actor.full_name or actor.email
    web._assert_accounting_date_open(db,web.local_today())
    trip=db.query(MotoTrip).filter_by(id=trip.id).populate_existing().with_for_update().one()
    if trip.accounting_entry_id:return
    original=trip.receipt
    if not original or not original.accounting_entry:raise ValueError('El viaje antiguo requiere revisión de su comprobante')
    if trip.status=='ANULADO':
        trip.accounting_entry_id=original.accounting_entry_id
        trip.reversal_entry_id=trip.reversal_receipt.accounting_entry_id if trip.reversal_receipt else None
    else:
        expense_entry(db,trip.branch_id,web.local_today(),trip.amount_cs,actor_name,f'AUTO-MOTO-ISOLATE-REV-{trip.id}',f'Separación de caja de ventas · viaje #{trip.id}',original=original.accounting_entry)
        trip.accounting_entry_id=expense_entry(db,trip.branch_id,web.local_today(),trip.amount_cs,actor_name,f'AUTO-MOTO-ISOLATE-{trip.id}',f'Transporte separado de ventas · viaje #{trip.id}').id
    for receipt in (original,trip.reversal_receipt):
        if not receipt or not receipt.afecta_caja:continue
        box=db.query(CajaDiaria).filter_by(branch_id=receipt.branch_id,bodega_id=receipt.bodega_id,fecha=receipt.fecha).with_for_update().first()
        if not box:raise ValueError('Revisa el saldo de caja del recibo anterior antes de reclasificar')
        sign=1 if receipt.tipo=='EGRESO' else -1
        box.saldo_cs=Decimal(str(box.saldo_cs or 0))+sign*Decimal(str(receipt.monto_cs or 0))
        box.saldo_usd=Decimal(str(box.saldo_usd or 0))+sign*Decimal(str(receipt.monto_usd or 0))
        receipt.afecta_caja=False
    db.add(MotoTripEvent(trip_id=trip.id,actor_id=actor.id if hasattr(actor,'id') else trip.rider_id,action='AISLADO',comment='Reclasificado a Banco. Recibos históricos excluidos de las cajas de ventas y efecto en saldo restaurado.',created_at=web.local_now_naive()))
    db.flush()
