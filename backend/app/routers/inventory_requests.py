"""Responsive Holl requisition panel, live stock search and guarded decisions."""
import hashlib
import hmac
import io
import json
import re
import time
import uuid
from urllib.parse import urlparse, quote
from fastapi import APIRouter, Depends, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload
from . import web as w
from ..core import inventory_requests as s
from ..models.inventory_requests import InventoryRequest

router=APIRouter()
PATH=s.PATH


def user_web(request:Request,db=Depends(w.get_db)):
    try:return w._require_user_web(request,db)
    except HTTPException as exc:
        if exc.status_code==302 and request.method=='GET':
            raise HTTPException(302,headers={'Location':'/login?next='+quote(request.url.path,safe='')})
        raise


def token(user,scope,nonce=None,stamp=None):
    payload=f'{nonce or uuid.uuid4().hex}.{stamp or int(time.time())}'
    sig=hmac.new(w.SECRET_KEY.encode(),f'requisition:{user.id}:{scope}:{payload}'.encode(),hashlib.sha256).hexdigest()
    return payload+'.'+sig


def validate(request,user,scope,value):
    origin=request.headers.get('origin')
    if origin and urlparse(origin).netloc!=request.url.netloc:
        raise HTTPException(403,'Origen del formulario inválido')
    value=str(value or '')
    if not re.fullmatch(r'[a-f0-9]{32}\.[0-9]{10}\.[a-f0-9]{64}',value):
        raise HTTPException(403,'Formulario inválido. Recarga la página.')
    nonce,stamp,_=value.split('.')
    if not -60<=time.time()-int(stamp)<=43200 or not hmac.compare_digest(value,token(user,scope,nonce,int(stamp))):
        raise HTTPException(403,'Formulario vencido. Recarga la página.')
    return nonce


def redirect(ident=None,error=''):
    return RedirectResponse(PATH+(f'/{ident}' if ident else '')+('?error='+quote(error) if error else ''),303)


@router.get(PATH)
def panel(request:Request,db=Depends(w.get_db),user=Depends(user_web)):
    s.enforce(user)
    dest,source=s.destination(db,user,request.query_params.get('destination'))
    status=request.query_params.get('status','pending')
    if status not in s.STATUSES|{'all'}:status='pending'
    try:page=max(1,min(int(request.query_params.get('page','1')),100000))
    except ValueError:page=1
    base=s.query(db,user)
    counts={key:base.filter(InventoryRequest.status==key).count() for key in s.STATUSES}
    if status!='all':base=base.filter(InventoryRequest.status==status)
    rows=base.options(selectinload(InventoryRequest.source),selectinload(InventoryRequest.destination)).order_by(InventoryRequest.created_at.desc(),InventoryRequest.id.desc()).offset((page-1)*30).limit(31).all()
    sellers=w._vendedores_for_bodega(db,dest)
    return request.app.state.templates.TemplateResponse('inventory_requests.html',dict(request=request,user=user,version=w.settings.UI_VERSION,admin=s.is_admin(user),dest=dest,source=source,warehouses=s.warehouses(db),sellers=sellers,today=w.local_today().isoformat(),token=token(user,'create'),rows=rows[:30],has_next=len(rows)>30,page=page,status=status,counts=counts,error=request.query_params.get('error',''),record=None))


@router.get(PATH+'/productos')
def products(request:Request,db=Depends(w.get_db),user=Depends(user_web)):
    dest,source=s.destination(db,user,request.query_params.get('destination'))
    q=w._ascii_lower(request.query_params.get('q','').strip()[:100])
    if len(q)<2:return JSONResponse({'items':[]},headers={'Cache-Control':'no-store'})
    words=q.split()
    candidates=db.query(w.Producto).filter(w.Producto.activo.is_(True),w.Producto.servicio_producto.is_(False)).all()
    matches=[p for p in candidates if all(word in w._ascii_lower(p.cod_producto+' '+p.descripcion) for word in words)]
    matches.sort(key=lambda p:(w._ascii_lower(p.cod_producto)!=q,not w._ascii_lower(p.cod_producto).startswith(q),p.cod_producto))
    matches=matches[:25]
    balances=w._balances_by_bodega(db,[dest.id,source.id],[p.id for p in matches])
    return JSONResponse({'items':[dict(id=p.id,code=p.cod_producto,description=p.descripcion,local=float(balances.get((p.id,dest.id),0)),source=float(balances.get((p.id,source.id),0))) for p in matches]},headers={'Cache-Control':'no-store'})


@router.post(PATH)
async def submit(request:Request,background:BackgroundTasks,db=Depends(w.get_db),user=Depends(user_web)):
    s.enforce(user)
    size=request.headers.get('content-length','0')
    if size.isdigit() and int(size)>65536:
        raise HTTPException(413,'La solicitud es demasiado grande')
    form=await request.form(max_fields=8,max_files=0)
    operation=validate(request,user,'create',form.get('token'))
    try:
        record,created=s.create(db,user,operation,form.get('destination_id'),form.get('seller_id'),str(form.get('items','')),form.get('observation',''),str(request.base_url))
        db.commit()
    except ValueError as exc:
        db.rollback();return redirect(error=str(exc))
    except IntegrityError:
        db.rollback()
        record=db.query(InventoryRequest).filter_by(operation_key=operation,requested_by=user.id).first()
        if not record:raise
        created=False
    if created:background.add_task(s.notify,record.id)
    return redirect(record.id)


@router.get(PATH+'/{ident:int}')
def detail(ident:int,request:Request,db=Depends(w.get_db),user=Depends(user_web)):
    record=s.get(db,user,ident)
    balances=s.balances(db,record) if record.status=='pending' else {}
    enough=all(balances.get((i.producto_id,record.source_id),0)>=i.quantity and i.product.activo and not i.product.servicio_producto for i in record.items)
    return request.app.state.templates.TemplateResponse('inventory_requests.html',dict(request=request,user=user,version=w.settings.UI_VERSION,admin=s.is_admin(user),record=record,balances=balances,enough=enough,can_cancel=s.is_admin(user) or record.requested_by==user.id,approve_token=token(user,f'approve:{ident}'),cancel_token=token(user,f'cancel:{ident}'),email_token=token(user,f'email:{ident}'),error=request.query_params.get('error','')))


@router.post(PATH+'/{ident:int}/autorizar')
async def approve(ident:int,request:Request,db=Depends(w.get_db),user=Depends(user_web)):
    s.enforce(user)
    if not s.is_admin(user):raise HTTPException(403,'Solo el administrador puede autorizar')
    form=await request.form(max_fields=2,max_files=0)
    validate(request,user,f'approve:{ident}',form.get('token'))
    try:
        s.approve(db,user,ident);db.commit()
    except Exception:
        db.rollback();raise
    return redirect(ident)


@router.post(PATH+'/{ident:int}/anular')
async def cancel(ident:int,request:Request,db=Depends(w.get_db),user=Depends(user_web)):
    s.enforce(user)
    form=await request.form(max_fields=3,max_files=0)
    validate(request,user,f'cancel:{ident}',form.get('token'))
    try:
        s.cancel(db,s.get(db,user,ident,lock=True),user,form.get('reason',''));db.commit()
    except ValueError as exc:
        db.rollback();return redirect(ident,str(exc))
    return redirect(ident)


@router.post(PATH+'/{ident:int}/correo')
async def retry_email(ident:int,request:Request,background:BackgroundTasks,db=Depends(w.get_db),user=Depends(user_web)):
    record=s.get(db,user,ident)
    if not s.is_admin(user) and record.requested_by!=user.id:raise HTTPException(403,'Acceso denegado')
    form=await request.form(max_fields=2,max_files=0)
    validate(request,user,f'email:{ident}',form.get('token'))
    if record.email_state!='sent':background.add_task(s.notify,ident)
    return redirect(ident)


@router.get(PATH+'/{ident:int}/recibo')
def receipt(ident:int,request:Request,db=Depends(w.get_db),user=Depends(user_web)):
    record=s.get(db,user,ident)
    if record.status!='active':raise HTTPException(409,'El comprobante está disponible después de autorizar')
    return request.app.state.templates.TemplateResponse('inventory_request_receipt.html',dict(request=request,user=user,record=record))


@router.get(PATH+'/{ident:int}/recibo.jpg')
def receipt_jpg(ident:int,request:Request,db=Depends(w.get_db),user=Depends(user_web)):
    record=s.get(db,user,ident)
    if record.status!='active':raise HTTPException(409,'El comprobante está disponible después de autorizar')
    from ..core.inventory_request_receipt import jpeg
    return Response(jpeg(record),media_type='image/jpeg',headers={'Content-Disposition':f'inline; filename={record.numero}.jpg','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})
