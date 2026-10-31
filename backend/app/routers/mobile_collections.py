"""Mobile-only user experience for Holl receivables and payment evidence."""
from datetime import date
from decimal import Decimal
from collections import defaultdict
from urllib.parse import urlparse
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse, Response
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import defer, joinedload
from . import web as w
from ..core import mobile_collections as service
from ..models.sales import MobileCollectionPayment

router = APIRouter()
BASE = '/m/cartera'


def metadata(db, ids):
    if not ids:
        return {}
    query = db.query(MobileCollectionPayment, MobileCollectionPayment.support.isnot(None)).filter(MobileCollectionPayment.abono_id.in_(ids)).options(defer(MobileCollectionPayment.support), joinedload(MobileCollectionPayment.user))
    return {item.abono_id: dict(record=item, has_support=has_support) for item, has_support in query.all()}


def history(db, user, vendor='', q='', start='', end='', page=1):
    permitted = service.invoice_query(db, user).with_entities(w.VentaFactura.id)
    query = db.query(w.CobranzaAbono).filter(w.CobranzaAbono.factura_id.in_(permitted)).join(w.VentaFactura)
    if service.is_admin(user) and vendor.isdigit():
        query = query.filter(w.VentaFactura.vendedor_id == int(vendor) if int(vendor) else w.VentaFactura.vendedor_id.is_(None))
    if q:
        query = query.join(w.Cliente, isouter=True).outerjoin(MobileCollectionPayment,MobileCollectionPayment.abono_id==w.CobranzaAbono.id).filter(w.or_(w.VentaFactura.numero.ilike('%'+q+'%'), w.Cliente.nombre.ilike('%'+q+'%'), w.CobranzaAbono.numero.ilike('%'+q+'%'), MobileCollectionPayment.reference.ilike('%'+q+'%'), MobileCollectionPayment.bank.ilike('%'+q+'%')))
    try:
        if start:query = query.filter(w.CobranzaAbono.fecha >= date.fromisoformat(start))
        if end:query = query.filter(w.CobranzaAbono.fecha <= date.fromisoformat(end))
        if start and end and start > end:raise ValueError()
    except ValueError:
        raise HTTPException(400, 'Revisa el rango de fechas del historial')
    return query.options(joinedload(w.CobranzaAbono.factura).joinedload(w.VentaFactura.cliente), joinedload(w.CobranzaAbono.factura).joinedload(w.VentaFactura.vendedor)).order_by(w.CobranzaAbono.fecha.desc(), w.CobranzaAbono.id.desc()).offset((page-1)*50).limit(51).all()


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
    rows = service.portfolio(db, user)
    totals = defaultdict(lambda: dict(cs=Decimal(0), usd=Decimal(0), count=0))
    vendors = {}
    for row in rows:
        f = row['invoice']; name = f.vendedor.nombre if f.vendedor else 'Sin asignar'
        key = f.vendedor_id or 0
        vendors[key] = name
        if row['due']:
            totals[key]['cs'] += row['due_cs']; totals[key]['usd'] += row['due_usd']; totals[key]['count'] += 1
    if vendor.isdigit(): rows = [r for r in rows if (r['invoice'].vendedor_id or 0) == int(vendor)]
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
    return render_page(request,db,user,review=True)


def detail(request, db, user, invoice_id, values=None, error='', status=200):
    f = service.invoice(db, user, invoice_id)
    payments = sorted(f.abonos, key=lambda a:(a.fecha,a.id), reverse=True)
    data = dict(token=service.form_token(user, f.id), date=w.local_today().isoformat(), currency=f.moneda or 'CS', method='deposito')
    data.update(values or {})
    return request.app.state.templates.TemplateResponse('mobile_collection_detail.html', dict(
        request=request, user=user, version=w.settings.UI_VERSION, admin=service.is_admin(user), invoice=f,
        balance=service.balance(f), payments=payments, metadata=metadata(db,[p.id for p in payments]),
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
        service.register(db,user,invoice_id,values,raw)
        db.commit()
    except ValueError as exc:
        db.rollback()
        return detail(request,db,user,invoice_id,values,str(exc),400)
    except IntegrityError:
        db.rollback()
        # A replay must belong to the same user and authorized invoice.
        f=service.invoice(db,user,invoice_id)
        key=service.operation_key(user,f.id,values['token'])
        existing=db.query(MobileCollectionPayment).join(w.CobranzaAbono).filter(MobileCollectionPayment.operation_key==key,MobileCollectionPayment.user_id==user.id,w.CobranzaAbono.factura_id==f.id).first()
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
