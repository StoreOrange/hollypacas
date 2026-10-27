"""Cola optativa por caja: factura una vez, imprime mediante el agente Windows."""
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4
from sqlalchemy import func
from fastapi import HTTPException
from ..models.pos_automation import PosAutoStation,PosAutoTask,PosPrintJob
from ..models.sales import Preventa,VentaFactura,PosPrintSetting,FormaPago,CierreCaja
from ..models.inventory import Bodega

DELAY=15


def lock_scope(db,station):
    # Mismo orden que la venta manual: bodega antes de registrar contabilidad.
    db.query(Bodega).filter_by(id=station.bodega_id).with_for_update().one()
    db.refresh(station)


def lock_station(db,station):
    """La cola/impresión solo serializa su estación; no bloquea ventas ni contabilidad."""
    db.query(PosAutoStation).filter_by(id=station.id).populate_existing().with_for_update().one()


def online(station,now):
    return bool(station.agent_ready and station.agent_seen and timedelta(0)<=now-station.agent_seen<timedelta(seconds=20))


def set_enabled(db,station,enabled,now,*,require_agent=True):
    lock_scope(db,station)
    if enabled:
        if require_agent and not online(station,now):raise ValueError('Instala y conecta el agente Windows con la Epson predeterminada antes de activar')
        others=db.query(PosAutoStation).filter(PosAutoStation.bodega_id==station.bodega_id,PosAutoStation.id!=station.id,PosAutoStation.enabled.is_(True)).all()
        for other in others:
            if other.last_tick and now-other.last_tick<timedelta(seconds=25):raise ValueError('Otra caja tiene activada la facturación automática de esta bodega')
            other.enabled=False
            db.query(PosAutoTask).filter_by(station_id=other.id,status='WAITING').update({'status':'CANCELLED','message':'Otra estación tomó el control'})
        if not station.enabled:
            station.baseline_id=db.query(func.max(Preventa.id)).filter_by(bodega_id=station.bodega_id).scalar() or 0
            station.enabled_at=now
        station.last_tick=now
    else:
        db.query(PosAutoTask).filter_by(station_id=station.id,status='WAITING').update({'status':'CANCELLED','message':'Automatización desactivada'})
    station.enabled=enabled
    db.flush()


def prepare(db,station,now):
    """Solo nuevas desde la activación; nunca facturar pendientes históricos al marcar."""
    if not station.enabled:return []
    from ..routers import web
    candidates=db.query(Preventa).filter(Preventa.bodega_id==station.bodega_id,Preventa.branch_id==station.branch_id,Preventa.id>station.baseline_id,Preventa.estado=='PENDIENTE',Preventa.is_frozen.is_(False),Preventa.venta_factura_id.is_(None),func.date(Preventa.fecha)==web.local_today(),~Preventa.id.in_(db.query(PosAutoTask.preventa_id))).order_by(Preventa.id).limit(100).all()
    for sale in candidates:
        db.add(PosAutoTask(station_id=station.id,preventa_id=sale.id,operation_key=uuid4().hex,due_at=now+timedelta(seconds=DELAY),status='WAITING'))
    db.flush()
    return db.query(PosAutoTask).filter_by(station_id=station.id,status='WAITING').order_by(PosAutoTask.due_at,PosAutoTask.id).all()


def form_pairs(task,preventa,method_id,today):
    pairs=[('operation_key',task.operation_key),('preventa_id',str(preventa.id)),('vendedor_id',str(preventa.vendedor_id)),('cliente_id',str(preventa.cliente_id or '')),('fecha',today.isoformat()),('moneda','CS'),('condicion_venta','CONTADO'),('forma_pago_id',str(method_id)),('observacion','Facturación automática de preventa · efectivo C$')]
    # No enviar monto: el flujo de venta toma exactamente el total calculado en servidor.
    for item in sorted(preventa.items,key=lambda i:i.id):
        for name,value in [('item_producto_id',item.producto_id),('item_cantidad',item.cantidad),('item_peso_lbs',item.peso_lbs or 0),('item_precio',item.precio_unitario_cs),('item_price_tier','1'),('item_role',item.combo_role or ''),('item_combo_group',item.combo_group or '')]:
            pairs.append((name,str(value)))
    return pairs


def queue_invoice(db,invoice,station_id,task_id):
    task=db.get(PosAutoTask,task_id);station=db.get(PosAutoStation,station_id)
    if not task or not station or task.station_id!=station.id or task.status!='WAITING':raise ValueError('La operación automática ya no está disponible')
    setting=db.query(PosPrintSetting).filter_by(branch_id=station.branch_id).first()
    copies=max(1,min(5,int(setting.copies or 1))) if setting else 1
    from ..routers import web
    copies = len(web._pos_invoice_copy_labels()) or copies
    db.add(PosPrintJob(station_id=station.id,invoice_id=invoice.id,copies=copies,status='QUEUED'))
    task.invoice_id=invoice.id;task.status='FACTURADA';task.message='Factura generada; impresión pendiente'
    db.flush()


async def invoice_due(db,station,task,request,user,now,*,require_agent=True):
    from ..routers import web
    from starlette.requests import Request
    from starlette.datastructures import FormData
    if task.status!='WAITING':return
    if task.due_at>now:raise ValueError(f'La cuenta regresiva de {DELAY} segundos no ha finalizado')
    if not station.enabled or (require_agent and not online(station,now)):return
    preventa=db.query(Preventa).filter_by(id=task.preventa_id).populate_existing().with_for_update().first()
    if not preventa or preventa.bodega_id!=station.bodega_id or preventa.branch_id!=station.branch_id:raise ValueError('Preventa fuera de la caja asignada')
    if preventa.fecha and preventa.fecha.date()!=web.local_today():
        task.status='CANCELLED';task.message='Preventa de un día anterior: revisión manual';db.flush();return
    if preventa.venta_factura_id or preventa.estado!='PENDIENTE' or preventa.is_frozen:
        task.status='CANCELLED';task.message='Preventa facturada, en revisión manual, anulada o congelada';db.flush();return
    if db.query(CierreCaja.id).filter_by(bodega_id=station.bodega_id,fecha=web.local_today()).first():raise ValueError('La caja del día está cerrada')
    methods=db.query(FormaPago).filter(func.lower(func.trim(FormaPago.nombre))=='efectivo').all()
    if len(methods)!=1:raise ValueError('Debe existir una única forma de pago llamada Efectivo')
    scope=dict(request.scope);scope['state']=dict(request.scope.get('state') or {})
    scope['state'].update(pos_auto_station_id=station.id,pos_auto_task_id=task.id)
    child=Request(scope);child._form=FormData(form_pairs(task,preventa,methods[0].id,web.local_today()))
    result=await web.sales_create_invoice(child,db,user)
    from urllib.parse import urlparse,parse_qs
    query=parse_qs(urlparse(result.headers.get('location','')).query)
    if query.get('error'):raise ValueError(query['error'][0])
    if not query.get('print_id'):raise ValueError('No se recibió confirmación de la factura; revisa la operación antes de reintentar')
