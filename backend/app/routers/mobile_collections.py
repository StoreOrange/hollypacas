"""Mobile-only user experience for Holl receivables and payment evidence."""
from datetime import date
from decimal import Decimal
from collections import defaultdict
from urllib.parse import urlparse
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse, Response
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import defer, joinedload, selectinload
from . import web as w
from ..core import mobile_collections as service
from ..core import debt_review, collection_carryover as tracking
from ..models.sales import MobileCollectionPayment, CollectionReviewSetting, CollectionCarryover, CollectionPanelPayment

router = APIRouter()
BASE = '/m/cartera'


def metadata(db, ids):
    if not ids:
        return {}
    query = db.query(CollectionPanelPayment).filter(CollectionPanelPayment.id.in_(ids))
    return {item.id: dict(record=item, has_support=has_support, url=f'{BASE}/comprobantes/{item.id}') for item, has_support in query.add_columns(CollectionPanelPayment.support.isnot(None)).options(defer(CollectionPanelPayment.support)).all()}


def history(db, user, vendor='', q='', start='', end='', page=1):
    query = tracking.payment_query(db,user).join(CollectionCarryover).join(w.VentaFactura)
    if service.is_admin(user) and vendor.isdigit():
        query=query.filter(CollectionCarryover.vendedor_id==int(vendor) if int(vendor) else CollectionCarryover.vendedor_id.is_(None))
    if q:
        query=query.outerjoin(w.Cliente).filter(w.or_(w.VentaFactura.numero.ilike('%'+q+'%'),w.Cliente.nombre.ilike('%'+q+'%'),CollectionPanelPayment.reference.ilike('%'+q+'%'),CollectionPanelPayment.bank.ilike('%'+q+'%')))
    try:
        if start:query=query.filter(CollectionPanelPayment.fecha>=date.fromisoformat(start))
        if end:query=query.filter(CollectionPanelPayment.fecha<=date.fromisoformat(end))
        if start and end and start>end:raise ValueError()
    except ValueError:raise HTTPException(400,'Revisa el rango de fechas del historial')
    return query.options(defer(CollectionPanelPayment.support),joinedload(CollectionPanelPayment.carryover).joinedload(CollectionCarryover.factura).joinedload(w.VentaFactura.cliente),joinedload(CollectionPanelPayment.carryover).joinedload(CollectionCarryover.vendedor)).order_by(CollectionPanelPayment.fecha.desc(),CollectionPanelPayment.id.desc()).offset((page-1)*50).limit(51).all()


def load_articles(db, invoices):
    ids=[f.id for f in invoices]
    if ids:
        db.query(w.VentaFactura).filter(w.VentaFactura.id.in_(ids)).options(selectinload(w.VentaFactura.items).joinedload(w.VentaItem.producto)).all()


def render_page(request, db, user, review=False):
    try:
        service.enforce(user)
    except HTTPException as exc:
        if exc.status_code != 403:raise
        return request.app.state.templates.TemplateResponse('mobile_collection_access.html',dict(request=request,user=user,version=w.settings.UI_VERSION,message=exc.detail),status_code=403)
    admin = service.is_admin(user)
    q = request.query_params.get('q', '').strip()[:100]
    vendor = request.query_params.get('vendor', '') if admin else str(user.vendedor_id)
    status = request.query_params.get('status', 'pending')
    view = request.query_params.get('view', 'portfolio')
    try:page_no=max(1,min(int(request.query_params.get('page','1')),100000))
    except ValueError:page_no=1
    start, end = request.query_params.get('from', ''), request.query_params.get('to', '')
    rows = tracking.portfolio(db, user)
    totals = defaultdict(lambda: dict(cs=Decimal(0), usd=Decimal(0), count=0))
    vendors = {}
    for row in rows:
        f = row['invoice']; name = row['vendor_name']
        key = row['vendor_id']
        vendors[key] = name
        if row['due']:
            totals[key]['cs'] += row['due_cs']; totals[key]['usd'] += row['due_usd']; totals[key]['count'] += 1
    if vendor.isdigit(): rows = [r for r in rows if r['vendor_id'] == int(vendor)]
    if q:
        rows = [r for r in rows if q.casefold() in (r['invoice'].numero+' '+(r['invoice'].cliente.nombre if r['invoice'].cliente else 'Consumidor final')).casefold()]
    pending = [r for r in rows if r['due']]
    total_cs = sum((r['due_cs'] for r in pending), Decimal(0))
    total_usd = sum((r['due_usd'] for r in pending), Decimal(0))
    if status == 'pending': rows = pending
    elif status == 'paid': rows = [r for r in rows if not r['due']]
    result_count=len(rows)
    has_next=result_count>page_no*50
    rows=rows[(page_no-1)*50:page_no*50]
    payments=history(db,user,vendor,q,start,end,page_no) if view=='history' else []
    if view=='history':has_next=len(payments)>50
    payments=payments[:50]
    return request.app.state.templates.TemplateResponse('mobile_collections.html', dict(
        request=request, user=user, version=w.settings.UI_VERSION, admin=admin, q=q, vendor=vendor,
        status=status, view=view, start=start, end=end, rows=rows, page_no=page_no,
        result_count=result_count, has_next=has_next, pending_count=len(pending), total_cs=total_cs,
        total_usd=total_usd, vendors=vendors, vendor_totals=totals, payments=payments,
        metadata=metadata(db,[p.id for p in payments]), detail=None, review=review, portfolio_path='/m/deudas' if review else BASE))


@router.get(BASE)
def page(request: Request, db=Depends(w.get_db), user=Depends(w._require_user_web)):
    return render_page(request,db,user)


@router.get('/m/deudas')
def review_page(request: Request, db=Depends(w.get_db), user=Depends(w._require_user_web)):
    return render_review(request,db,user)


def render_review(request,db,user,error='',status_code=200):
    try:service.enforce(user)
    except HTTPException as exc:
        if exc.status_code!=403:raise
        return request.app.state.templates.TemplateResponse('mobile_collection_access.html',dict(request=request,user=user,version=w.settings.UI_VERSION,message=exc.detail),status_code=403)
    admin=service.is_admin(user);config=debt_review.settings(db)
    vendor=request.query_params.get('vendor','') if admin else str(user.vendedor_id)
    client=request.query_params.get('client','')
    if any(value and not value.isdigit() for value in (vendor,client)):
        raise HTTPException(400,'Selecciona un vendedor o cliente válido')
    if client and not vendor:raise HTTPException(400,'Selecciona primero un vendedor')
    mode=request.query_params.get('view','summary')
    if mode=='history':mode='recent'
    if mode not in {'summary','recent','aged'}:mode='summary'
    q=request.query_params.get('q','').strip()[:100]
    all_rows=tracking.portfolio(db,user)
    vendor_options={r['vendor_id']:r['vendor_name'] for r in all_rows}
    if not admin and user.vendedor_id not in vendor_options:
        vendor_options[user.vendedor_id]=user.vendedor.nombre if user.vendedor else 'Mi cartera'
    if vendor and int(vendor) not in vendor_options:raise HTTPException(404,'Vendedor no encontrado en tu cartera')
    rows=[r for r in all_rows if not vendor or r['vendor_id']==int(vendor)]
    if client:
        rows=[r for r in rows if (r['invoice'].cliente_id or 0)==int(client)]
        if not rows:raise HTTPException(404,'Cliente no encontrado en tu cartera')
    if q:
        query=w._ascii_lower(q)
        rows=[r for r in rows if query in w._ascii_lower(r['invoice'].numero+' '+(r['invoice'].cliente.nombre if r['invoice'].cliente else 'Consumidor final')+' '+(r['invoice'].vendedor.nombre if r['invoice'].vendedor else ''))]
    data=debt_review.build(rows,w.local_today(),config)
    stats=debt_review.blank()
    for group in data['clients']:
        for key in stats:stats[key]+=group[key]
    selected_client=data['clients'][0] if client and data['clients'] else None
    if mode=='recent':items=[e for e in data['receipts'] if e['recent']];level='recent'
    elif mode=='aged':items=sorted([r for r in data['invoices'] if r['aged']],key=lambda r:(-r['age'],-r['due_cs']));level='aged'
    elif client:items=data['invoices'];level='invoices'
    elif vendor:items=data['clients'];level='clients'
    else:items=data['vendors'];level='vendors'
    focus=request.query_params.get('invoice','')
    if focus and level=='invoices':
        if not focus.isdigit() or int(focus) not in {r['invoice'].id for r in items}:raise HTTPException(404,'Factura no encontrada en este cliente')
        items=sorted(items,key=lambda r:r['invoice'].id!=int(focus))
    try:page_no=max(1,min(int(request.query_params.get('page','1')),100000))
    except ValueError:page_no=1
    count=len(items);items=items[(page_no-1)*20:page_no*20]
    visible=[]
    if level=='recent':visible=items
    elif level=='invoices':visible=[e for r in items for e in r['events']]
    if level=='invoices':load_articles(db,[r['invoice'] for r in items])
    meta=metadata(db,[e['abono_id'] for e in visible if e['abono_id']])
    return request.app.state.templates.TemplateResponse('debt_review.html',dict(
        request=request,user=user,version=w.settings.UI_VERSION,admin=admin,config=config,palette=debt_review.PALETTE,
        token=service.form_token(user,-1) if admin else '',vendor=vendor,client=client,mode=mode,q=q,
        vendor_options=vendor_options,vendor_name=vendor_options.get(int(vendor)) if vendor else '',
        selected_client=selected_client,stats=stats,items=items,level=level,count=count,page_no=page_no,
        has_next=count>page_no*20,metadata=meta,focus=focus,error=error,configured=request.query_params.get('configured')=='1',tracking_epoch=tracking.epoch(db)),status_code=status_code)


@router.post('/m/deudas/configuracion')
async def configure_review(request:Request,db=Depends(w.get_db),user=Depends(w._require_user_web)):
    service.enforce(user)
    if not service.is_admin(user):raise HTTPException(403,'Solo el administrador puede configurar las alertas')
    origin=request.headers.get('origin')
    if origin and urlparse(origin).netloc!=request.url.netloc:raise HTTPException(403,'Origen del formulario inválido')
    form=await request.form(max_fields=5,max_files=0)
    try:
        try:service.operation_key(user,-1,str(form.get('token','')))
        except ValueError:raise ValueError('Recarga el panel y vuelve a guardar la configuración.')
        raw={key:str(form.get(key,'')).strip() for key in ('overdue_days','recent_days')}
        if any(not value.isdigit() for value in raw.values()):raise ValueError('Escribe los días como números enteros entre 1 y 3650.')
        numbers={key:int(value) for key,value in raw.items()}
        if any(not 1<=value<=3650 for value in numbers.values()):raise ValueError('Los días deben estar entre 1 y 3650')
        colors={key:str(form.get(key,'')) for key in ('recent_color','aged_color')}
        if any(value not in debt_review.PALETTE for value in colors.values()):raise ValueError('Selecciona los colores disponibles')
        if db.get_bind().dialect.name=='postgresql':db.execute(w.sql_text('SELECT pg_advisory_xact_lock(83742032)'))
        setting=db.query(CollectionReviewSetting).filter_by(id=1).with_for_update().first()
        if not setting:setting=CollectionReviewSetting(id=1);db.add(setting)
        for key,value in dict(numbers,**colors).items():setattr(setting,key,value)
        setting.updated_by=user.id;db.commit()
    except ValueError as exc:
        db.rollback();return render_review(request,db,user,str(exc),400)
    return RedirectResponse('/m/deudas?configured=1',status_code=303)


def detail(request, db, user, invoice_id, values=None, error='', status=200):
    record = tracking.get(db,user,invoice_id)
    f = record.factura
    load_articles(db,[f])
    payments = sorted(record.payments, key=lambda a:(a.fecha,a.id), reverse=True)
    data = dict(token=service.form_token(user, f.id), date=w.local_today().isoformat(), currency=f.moneda or 'CS', method='deposito')
    data.update(values or {})
    return request.app.state.templates.TemplateResponse('mobile_collection_detail.html', dict(
        request=request, user=user, version=w.settings.UI_VERSION, admin=service.is_admin(user), invoice=f,
        balance=tracking.balance(record), payments=payments, metadata=metadata(db,[p.id for p in payments]),
        methods=service.METHODS, values=data, error=error, saved=request.query_params.get('saved')=='1',
        today=w.local_today().isoformat()), status_code=status)


@router.get(BASE+'/{invoice_id:int}')
def invoice_page(request: Request, invoice_id: int, db=Depends(w.get_db), user=Depends(w._require_user_web)):
    return detail(request,db,user,invoice_id)


@router.post(BASE+'/{invoice_id:int}/pagos')
async def create(request: Request, invoice_id: int, db=Depends(w.get_db), user=Depends(w._require_user_web)):
    service.enforce(user)
    origin = request.headers.get('origin')
    if origin and urlparse(origin).netloc != request.url.netloc:
        raise HTTPException(403, 'Origen del formulario inválido')
    if request.headers.get('content-length','').isdigit() and int(request.headers['content-length'])>service.MAX_UPLOAD+65536:
        raise HTTPException(413,'Adjunta una imagen de hasta 8 MB')
    form = await request.form(max_files=1, max_fields=12)
    values = {key: str(form.get(key, '')) for key in ('token','date','currency','amount','method','bank','reference','note')}
    upload = form.get('support')
    raw = b''
    try:
        if upload and getattr(upload,'filename',''):
            raw = await upload.read(service.MAX_UPLOAD+1)
            if len(raw)>service.MAX_UPLOAD:raise ValueError('La imagen debe pesar hasta 8 MB.')
        tracking.register(db,user,invoice_id,values,raw)
        db.commit()
    except ValueError as exc:
        db.rollback()
        return detail(request,db,user,invoice_id,values,str(exc),400)
    except IntegrityError:
        db.rollback()
        # A replay must belong to the same user and authorized invoice.
        record=tracking.get(db,user,invoice_id)
        key=service.operation_key(user,invoice_id,values['token'])
        existing=tracking.payment_query(db,user).filter(CollectionPanelPayment.operation_key==key,CollectionPanelPayment.user_id==user.id,CollectionPanelPayment.carryover_id==record.id).first()
        if not existing:return detail(request,db,user,invoice_id,values,'No se pudo registrar; revisa el historial antes de reintentar.',409)
    finally:
        if upload and hasattr(upload,'close'):await upload.close()
    return RedirectResponse(f'{BASE}/{invoice_id}?saved=1',status_code=303)


@router.get(BASE+'/soportes/{payment_id:int}')
def support(request: Request, payment_id: int, db=Depends(w.get_db), user=Depends(w._require_user_web)):
    service.enforce(user)
    permitted=service.invoice_query(db,user).with_entities(w.VentaFactura.id)
    item=db.query(MobileCollectionPayment).join(w.CobranzaAbono).filter(MobileCollectionPayment.id==payment_id,w.CobranzaAbono.factura_id.in_(permitted)).first()
    if not item or not item.support:raise HTTPException(404,'Soporte no encontrado')
    return Response(item.support,media_type='image/jpeg',headers={'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff','Content-Disposition':f'inline; filename=soporte-{item.id}.jpg'})


@router.get(BASE+'/comprobantes/{payment_id:int}')
def panel_support(request:Request,payment_id:int,db=Depends(w.get_db),user=Depends(w._require_user_web)):
    item=tracking.payment_query(db,user).filter(CollectionPanelPayment.id==payment_id).first()
    if not item or not item.support:raise HTTPException(404,'Soporte no encontrado')
    return Response(item.support,media_type='image/jpeg',headers={'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff','Content-Disposition':f'inline; filename=comprobante-{item.id}.jpg'})
