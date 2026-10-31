"""Auditable owner-authorized portfolio closure; never records fictitious cash."""
import json
from decimal import Decimal
from sqlalchemy import func, text
from sqlalchemy.orm import selectinload
from . import mobile_collections as service
from ..routers import web as w
from ..models.sales import MobileCollectionBasis


def close_pending(db, user, protected_ids, marker):
    service.enforce(user)
    if not service.is_admin(user):
        raise w.HTTPException(403,'Solo el administrador puede cerrar cartera')
    protected_ids={int(id) for id in protected_ids}
    if db.get_bind().dialect.name=='postgresql':
        db.execute(text('SELECT pg_advisory_xact_lock(83742031)'))
    invoices=db.query(w.VentaFactura).options(selectinload(w.VentaFactura.pagos),selectinload(w.VentaFactura.abonos),selectinload(w.VentaFactura.mobile_collection_basis)).filter(w.VentaFactura.estado_cobranza=='PENDIENTE').order_by(w.VentaFactura.id).with_for_update().all()
    protected={f.id for f in invoices if f.id in protected_ids and f.estado!='ANULADA'}
    if protected!=protected_ids:
        raise ValueError('Una factura protegida no existe o ya no está pendiente. No se aplicó el cierre.')
    targets=[f for f in invoices if f.id not in protected_ids]
    warehouse_ids=sorted({f.bodega_id for f in targets if f.estado!='ANULADA' and f.bodega_id})
    warehouses={b.id:b for b in db.query(w.Bodega).filter(w.Bodega.id.in_(warehouse_ids)).order_by(w.Bodega.id).with_for_update().all()}
    seqs={id:int(seq or 0) for id,seq in db.query(w.CobranzaAbono.bodega_id,func.max(w.CobranzaAbono.secuencia)).filter(w.CobranzaAbono.bodega_id.in_(warehouse_ids)).group_by(w.CobranzaAbono.bodega_id).all()}
    audit=[];notes=[]
    for f in targets:
        entry=dict(id=f.id,numero=f.numero,estado=f.estado,estado_cobranza=f.estado_cobranza,moneda=f.moneda,total_usd=str(f.total_usd),total_cs=str(f.total_cs))
        if f.estado!='ANULADA':
            ignored=service.deferred_initial_payments(f)
            paid_usd,paid_cs=w._cobranza_abonos_totals(list(f.abonos))
            for p in f.pagos:
                if p.id not in ignored:
                    paid_usd+=Decimal(str(p.monto_usd or 0));paid_cs+=Decimal(str(p.monto_cs or 0))
            usd=max(Decimal(str(f.total_usd or 0))-paid_usd,Decimal(0)).quantize(service.CENT)
            cs=max(Decimal(str(f.total_cs or 0))-paid_cs,Decimal(0)).quantize(service.CENT)
            entry.update(ignored_payment_ids=sorted(ignored),saldo_usd=str(usd),saldo_cs=str(cs),abonos_previos=[a.id for a in f.abonos],pagos_originales=[p.id for p in f.pagos])
            if usd or cs:
                b=warehouses.get(f.bodega_id)
                if not b or not b.branch:
                    raise ValueError(f'Factura {f.numero} sin bodega válida; cierre cancelado')
                if ignored and not f.mobile_collection_basis:
                    f.mobile_collection_basis=MobileCollectionBasis(ignored_payment_ids=json.dumps(sorted(ignored)),user_id=user.id)
                seqs[b.id]=seqs.get(b.id,0)+1
                prefix,series=w._branch_cobranza_series_meta(b.branch.code.lower())
                moneda=(f.moneda or 'CS') if (usd if f.moneda=='USD' else cs)>0 else ('USD' if usd else 'CS')
                note=w.CobranzaAbono(factura_id=f.id,branch_id=b.branch_id,bodega_id=b.id,secuencia=seqs[b.id],numero=f'{prefix}-{series}-{seqs[b.id]:04d}',fecha=w.local_today(),tipo_mov='NOTA_CREDITO',moneda=moneda,tasa_cambio=(cs/usd).quantize(Decimal('.0001')) if usd and cs else f.tasa_cambio,monto_usd=usd,monto_cs=cs,afecta_caja=False,observacion=f'Cierre administrativo de cartera autorizado por propietario. {marker}',usuario_registro=user.full_name or user.email)
                db.add(note);notes.append(note)
                entry['ajuste_numero']=note.numero
        f.estado_cobranza='PAGADA';audit.append(entry)
    db.flush()
    adjustment_ids=[n.id for n in notes]
    # Refresh collections populated before the adjustment insert.
    db.expire_all()
    for f in service.loaded(db.query(w.VentaFactura).filter(w.VentaFactura.id.in_([a['id'] for a in audit if a['estado']!='ANULADA']))).all():
        if service.balance(f)['due']:
            raise ValueError('El cierre no cuadró; no se debe confirmar la transacción')
    remaining=[f.id for f in db.query(w.VentaFactura).filter(w.VentaFactura.estado_cobranza=='PENDIENTE').all()]
    if set(remaining)!=protected_ids:
        raise ValueError('Los pendientes cambiaron durante el proceso; no confirmar')
    return dict(marker=marker,closed=len(audit),adjustments=len(notes),protected=sorted(protected_ids),remaining=sorted(remaining),invoices=audit,adjustment_ids=adjustment_ids)
