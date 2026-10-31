"""Independent vendor-remittance control: only balances carried by a cash close."""
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import func, text
from sqlalchemy.orm import selectinload, joinedload
from ..models.sales import CollectionTrackingEpoch, CollectionCarryover, CollectionPanelPayment
from ..routers import web as w
from . import mobile_collections as general


def lock_setup(db):
    if db.get_bind().dialect.name=='postgresql':db.execute(text('SELECT pg_advisory_xact_lock(83742033)'))


def epoch(db):return db.get(CollectionTrackingEpoch,1)


def enroll_close(db,close):
    start=epoch(db)
    if not start or not w._is_pacasholl_company():return []
    lock_setup(db)
    query=db.query(w.VentaFactura).filter(w.VentaFactura.bodega_id==close.bodega_id,w.VentaFactura.estado!='ANULADA',
        w.VentaFactura.estado_cobranza=='PENDIENTE',func.date(w.VentaFactura.fecha)<=close.fecha,
        w.VentaFactura.created_at<=close.created_at,
        ~w.VentaFactura.id.in_(db.query(CollectionCarryover.factura_id)))
    rows=general.loaded(query.with_for_update(of=w.VentaFactura)).all()
    created=[]
    for f in rows:
        pending=general.balance(f)
        if not pending['due']:continue
        record=CollectionCarryover(factura_id=f.id,epoch_id=start.id,cierre_id=close.id,vendedor_id=f.vendedor_id,
            moneda=pending['currency'],opening_usd=pending['due_usd'],opening_cs=pending['due_cs'],
            eligible_on=max(close.fecha+timedelta(days=1),start.start_date))
        db.add(record);created.append(record)
    db.flush()
    return created


def initialize(db,user,start_date):
    general.enforce(user)
    if not general.is_admin(user):raise w.HTTPException(403,'Solo el administrador puede iniciar el seguimiento')
    lock_setup(db)
    existing=epoch(db)
    if existing:return existing,[]
    record=CollectionTrackingEpoch(id=1,start_date=start_date,started_by=user.id);db.add(record);db.flush()
    latest_prior={};today_closes=[]
    for close in db.query(w.CierreCaja).filter(w.CierreCaja.fecha<=start_date).order_by(w.CierreCaja.fecha.desc(),w.CierreCaja.id.desc()).all():
        if close.fecha<start_date:latest_prior.setdefault(close.bodega_id,close)
        else:today_closes.append(close)
    created=[]
    # Preserve debts already carried into today before queuing today's new debts.
    for close in latest_prior.values():created.extend(enroll_close(db,close))
    for close in reversed(today_closes):created.extend(enroll_close(db,close))
    return record,created


def query(db,user,available=True):
    general.enforce(user)
    q=db.query(CollectionCarryover).join(w.VentaFactura,CollectionCarryover.factura_id==w.VentaFactura.id).join(w.Bodega,w.Bodega.id==w.VentaFactura.bodega_id).join(w.Branch,w.Branch.id==w.Bodega.branch_id)
    q=q.filter(CollectionCarryover.epoch_id==1,func.lower(w.Branch.code).in_(w._allowed_branch_codes(db)))
    if available:q=q.filter(CollectionCarryover.eligible_on<=w.local_today())
    if not general.is_admin(user):q=q.filter(CollectionCarryover.vendedor_id==user.vendedor_id)
    return q


def loaded(q):
    return q.options(selectinload(CollectionCarryover.payments).defer(CollectionPanelPayment.support),
        joinedload(CollectionCarryover.factura).joinedload(w.VentaFactura.cliente),
        joinedload(CollectionCarryover.factura).joinedload(w.VentaFactura.bodega),
        joinedload(CollectionCarryover.vendedor),joinedload(CollectionCarryover.cierre))


def get(db,user,invoice_id,lock=False):
    q=loaded(query(db,user).filter(CollectionCarryover.factura_id==invoice_id))
    if lock:q=q.with_for_update(of=CollectionCarryover).populate_existing()
    record=q.first()
    if not record:raise w.HTTPException(404,'Esta factura aún no tiene un saldo trasladado del cierre a tu cartera')
    return record


def balance(record):
    paid_usd=sum((Decimal(str(p.monto_usd)) for p in record.payments),Decimal(0))
    paid_cs=sum((Decimal(str(p.monto_cs)) for p in record.payments),Decimal(0))
    total=Decimal(str(record.opening_usd if record.moneda=='USD' else record.opening_cs))
    paid=paid_usd if record.moneda=='USD' else paid_cs
    due=max(total-paid,Decimal(0)).quantize(general.CENT)
    ratio=due/total if total else Decimal(0)
    return dict(currency=record.moneda,total=total,paid=paid,due=due,
        due_cs=(record.opening_cs*ratio).quantize(general.CENT),due_usd=(record.opening_usd*ratio).quantize(general.CENT),
        deferred_ids=set(),carryover=record,panel_payments=list(record.payments),vendor_id=record.vendedor_id or 0,
        vendor_name=record.vendedor.nombre if record.vendedor else 'Sin vendedor asignado',
        tracking_date=record.eligible_on)


def portfolio(db,user):
    return [dict(invoice=r.factura,**balance(r)) for r in loaded(query(db,user)).order_by(CollectionCarryover.eligible_on.desc(),CollectionCarryover.id.desc()).all()]


def payment_query(db,user):
    permitted=query(db,user).with_entities(CollectionCarryover.id)
    return db.query(CollectionPanelPayment).filter(CollectionPanelPayment.carryover_id.in_(permitted))


def register(db,user,invoice_id,values,image=b''):
    general.enforce(user)
    key=general.operation_key(user,invoice_id,values.get('token'))
    record=get(db,user,invoice_id,lock=True)
    old=db.query(CollectionPanelPayment).filter_by(operation_key=key).first()
    if old:
        if old.user_id!=user.id or old.carryover_id!=record.id:raise ValueError('La solicitud no corresponde a esta deuda')
        return old
    pending=balance(record)
    if not pending['due']:raise ValueError('Esta deuda de cierre ya está saldada')
    amount=general.amount(values.get('amount'))
    currency=values.get('currency','');method=values.get('method','')
    bank=str(values.get('bank','')).strip();reference=str(values.get('reference','')).strip();note=str(values.get('note','')).strip()
    if currency not in {'CS','USD'} or method not in general.METHODS:raise ValueError('Selecciona moneda y forma de pago válidas')
    if len(bank)>100 or len(reference)>100 or len(note)>300:raise ValueError('Acorta el banco, referencia u observación')
    try:paid_date=date.fromisoformat(str(values.get('date','')))
    except ValueError:raise ValueError('Selecciona una fecha de pago válida')
    if paid_date>w.local_today() or paid_date<record.eligible_on:raise ValueError('La fecha del pago debe estar entre el inicio de este seguimiento y hoy')
    if method!='efectivo' and (not bank or not reference or not image):raise ValueError('Indica banco, referencia y foto de soporte')
    if method!='efectivo' and db.query(CollectionPanelPayment.id).filter(CollectionPanelPayment.carryover_id==record.id,func.lower(CollectionPanelPayment.bank)==bank.lower(),func.lower(CollectionPanelPayment.reference)==reference.lower()).first():raise ValueError('La referencia ya está registrada en esta deuda')
    support=general.clean_image(image) if image else None
    rate=db.query(w.ExchangeRate).filter(w.ExchangeRate.effective_date<=paid_date).order_by(w.ExchangeRate.effective_date.desc(),w.ExchangeRate.id.desc()).first()
    rate=Decimal(str(rate.rate or 0)) if rate else Decimal(0)
    if rate<=0:raise ValueError('No hay tasa de cambio válida para esa fecha')
    usd=amount if currency=='USD' else (amount/rate).quantize(general.CENT,rounding=ROUND_HALF_UP)
    cs=amount if currency=='CS' else (amount*rate).quantize(general.CENT,rounding=ROUND_HALF_UP)
    raw_native=amount if currency==record.moneda else (amount/rate if record.moneda=='USD' else amount*rate)
    native=usd if record.moneda=='USD' else cs
    if native<=0 or raw_native>pending['due']:raise ValueError(f"El pago supera el saldo de {record.moneda} {pending['due']:,.2f} o es demasiado pequeño")
    result=CollectionPanelPayment(operation_key=key,carryover_id=record.id,user_id=user.id,fecha=paid_date,moneda=currency,
        tasa_cambio=rate,monto_usd=usd,monto_cs=cs,method=method,bank=bank,reference=reference,
        observacion=note,usuario_registro=user.full_name or user.email,support=support)
    db.add(result);db.flush()
    return result


def recover_legacy_payment(db,user,payment_id):
    """Recover a verified pre-cutover mobile receipt for a debt carried by a close.

    General collection receipts and same-business-day sales never qualify.
    Reuse the original operation key so retries cannot duplicate the receipt.
    """
    from types import SimpleNamespace
    from ..models.sales import MobileCollectionPayment
    general.enforce(user)
    if not general.is_admin(user):raise w.HTTPException(403,'Solo el administrador puede recuperar un pago')
    lock_setup(db)
    start=epoch(db)
    if not start:raise ValueError('El seguimiento aún no está iniciado')
    source=db.query(MobileCollectionPayment).filter_by(id=payment_id).first()
    if not source:raise ValueError('No existe ese pago de cartera móvil')
    existing=db.query(CollectionPanelPayment).filter_by(operation_key=source.operation_key).first()
    if existing:return existing
    a=source.abono
    if a.tipo_mov!='ABONO' or a.fecha<start.start_date or not a.created_at or not source.created_at or source.created_at>start.created_at:
        raise ValueError('Solo se recuperan abonos de cartera del inicio, anteriores al cambio de versión')
    f=general.loaded(db.query(w.VentaFactura).filter_by(id=a.factura_id).with_for_update(of=w.VentaFactura)).first()
    # Scope and the physical execution time of the close both matter.
    if not general.invoice_query(db,user).filter(w.VentaFactura.id==f.id).first() or f.estado=='ANULADA':
        raise ValueError('La factura no pertenece a una cartera válida')
    close=db.query(w.CierreCaja).filter(w.CierreCaja.bodega_id==f.bodega_id,
        w.CierreCaja.fecha>=f.fecha.date(),w.CierreCaja.fecha<a.fecha,
        w.CierreCaja.created_at>=f.created_at,w.CierreCaja.created_at<a.created_at,
        w.CierreCaja.created_at<source.created_at).order_by(w.CierreCaja.fecha.desc(),w.CierreCaja.id.desc()).first()
    if not close:raise ValueError('El pago no corresponde a una deuda pendiente del día siguiente a un cierre ejecutado')
    # Reconstruct only the balance at the close, without changing ORM collections.
    if any(not item.created_at for item in f.abonos):raise ValueError('Faltan fechas de auditoría para reconstruir el cierre')
    snapshot=SimpleNamespace(moneda=f.moneda,total_cs=f.total_cs,total_usd=f.total_usd,
        pagos=f.pagos,mobile_collection_basis=f.mobile_collection_basis,estado_cobranza='PENDIENTE',
        abonos=[item for item in f.abonos if item.created_at<=close.created_at])
    opening=general.balance(snapshot)
    native=Decimal(str(a.monto_usd if opening['currency']=='USD' else a.monto_cs))
    if native<=0 or native>opening['due']:raise ValueError('El abono no coincide con el saldo pendiente del cierre')
    record=db.query(CollectionCarryover).filter_by(factura_id=f.id).with_for_update().first()
    if record:
        if record.cierre_id!=close.id or record.opening_usd!=opening['due_usd'] or record.opening_cs!=opening['due_cs']:
            raise ValueError('Ya existe otro saldo de seguimiento; requiere revisión antes de recuperar el pago')
        if native>balance(record)['due']:raise ValueError('El pago ya no cabe en el saldo del seguimiento')
    else:
        record=CollectionCarryover(factura_id=f.id,epoch_id=start.id,cierre_id=close.id,
            vendedor_id=source.vendedor_id,moneda=opening['currency'],opening_usd=opening['due_usd'],
            opening_cs=opening['due_cs'],eligible_on=max(close.fecha+timedelta(days=1),start.start_date))
        db.add(record);db.flush()
    rate=a.tasa_cambio or f.tasa_cambio
    if not rate and a.monto_cs and a.monto_usd:rate=a.monto_cs/a.monto_usd
    if not rate or rate<=0:raise ValueError('El pago original no tiene una tasa válida')
    result=CollectionPanelPayment(operation_key=source.operation_key,carryover_id=record.id,user_id=source.user_id,
        fecha=a.fecha,moneda=a.moneda,tasa_cambio=rate,
        monto_usd=a.monto_usd,monto_cs=a.monto_cs,method=source.method,bank=source.bank,
        reference=source.reference,observacion=(f'Recibo original de cartera: {a.numero}. '+(a.observacion or ''))[:300],
        usuario_registro=a.usuario_registro or source.user.full_name or source.user.email,
        support=source.support,created_at=source.created_at)
    db.add(result);db.flush()
    return result
