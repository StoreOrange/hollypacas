from datetime import date
from uuid import uuid4
from urllib.parse import quote_plus
import hashlib,hmac
from fastapi import APIRouter,Request,Depends,HTTPException
from fastapi.responses import RedirectResponse,Response
from sqlalchemy import func,case
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload,selectinload
from ..core.deps import get_db
from ..core.security import SECRET_KEY
from ..core import motorizado as service
from ..core import motorizado_photos as photos
from ..models.motorizado import MotoTrip,MotoTripEvent,MotoTripPhoto,MotoTripClosure
from ..models.sales import Vendedor,CuentaContable
from ..models.user import User
from . import web

router=APIRouter(tags=['Motorizado Hollpacas'])


def access(request:Request,db=Depends(get_db)):
    if not web._is_pacasholl_company():raise HTTPException(404)
    user=web._require_user_web(request,db)
    if not service.roles(user)&{'administrador','vendedor','motorizado'}:raise HTTPException(403,'Tu rol no tiene acceso a Motorizado')
    return user


def csrf(request,user):
    return hmac.new(str(SECRET_KEY).encode(),f'moto:{user.id}:{request.cookies.get("access_token", "")}'.encode(),hashlib.sha256).hexdigest()


async def form_data(request:Request):return await request.form(max_files=8,max_fields=24)


def verify(request,user,form):
    if not hmac.compare_digest(str(form.get('csrf') or ''),csrf(request,user)):raise HTTPException(403,'El formulario venció; recarga y vuelve a intentar')


def redirect(message,error=False,day=None):
    suffix=f'&from={day.isoformat()}&to={day.isoformat()}' if day else ''
    return RedirectResponse('/motorizado?'+('error=' if error else 'ok=')+quote_plus(str(getattr(message,'detail',message)))+suffix,status_code=303)


@router.get('/motorizado')
def home(request:Request,db=Depends(get_db),user=Depends(access)):
    today=web.local_today();names=service.roles(user);admin='administrador' in names
    view=request.query_params.get('view','costos' if 'vendedor' in names and 'motorizado' not in names else 'viajes')
    if view=='cierres':return closure_report(request,db,user)
    history=request.query_params.get('history')=='1'
    dates=service.scope(db,user,db.query(func.min(MotoTrip.trip_date)))
    if not history:dates=dates.filter(MotoTrip.closure_id.is_(None),MotoTrip.status!='ANULADO')
    default_start=dates.scalar() or today
    try:
        start=date.fromisoformat(request.query_params.get('from') or str(default_start or today));end=date.fromisoformat(request.query_params.get('to') or str(today))
        if start>end:raise ValueError()
        page=max(1,int(request.query_params.get('page') or 1))
    except ValueError:raise HTTPException(400,'Revisa las fechas y la página solicitada')
    query=service.scope(db,user,db.query(MotoTrip)).filter(MotoTrip.trip_date.between(start,end))
    if not history:query=query.filter(MotoTrip.closure_id.is_(None),MotoTrip.status!='ANULADO')
    # Cada panel conserva su identidad incluso cuando el usuario tiene ambos roles.
    if not admin:
        if view=='costos':query=query.filter(MotoTrip.seller_id==user.vendedor_id) if user.vendedor_id and 'vendedor' in names else query.filter(MotoTrip.id<0)
        else:query=query.filter(MotoTrip.rider_id==user.id) if 'motorizado' in names else query.filter(MotoTrip.id<0)
    seller=request.query_params.get('seller','');rider=request.query_params.get('rider','');status=request.query_params.get('status','')
    if seller.isdigit():query=query.filter(MotoTrip.seller_id==int(seller))
    if rider.isdigit() and admin:query=query.filter(MotoTrip.rider_id==int(rider))
    if status in {'PENDIENTE','ENTREGADO','RECIBIDO','ANULADO'}:query=query.filter(MotoTrip.status==status)
    count,total,delivered,pending,canceled=query.with_entities(func.count(MotoTrip.id),func.coalesce(func.sum(case((MotoTrip.status!='ANULADO',MotoTrip.amount_cs),else_=0)),0),func.coalesce(func.sum(case((MotoTrip.status.in_(['ENTREGADO','RECIBIDO']),1),else_=0)),0),func.coalesce(func.sum(case((MotoTrip.status=='PENDIENTE',1),else_=0)),0),func.coalesce(func.sum(case((MotoTrip.status=='ANULADO',MotoTrip.amount_cs),else_=0)),0)).one()
    pages=max(1,(count+29)//30);page=min(page,pages)
    trips=query.options(joinedload(MotoTrip.rider),joinedload(MotoTrip.seller),joinedload(MotoTrip.closure),selectinload(MotoTrip.events).joinedload(MotoTripEvent.actor),selectinload(MotoTrip.photos)).order_by(MotoTrip.created_at.desc(),MotoTrip.id.desc()).offset((page-1)*30).limit(30).all()
    costs=query.join(Vendedor,Vendedor.id==MotoTrip.seller_id).with_entities(Vendedor.nombre,func.count(MotoTrip.id),func.coalesce(func.sum(case((MotoTrip.status!='ANULADO',MotoTrip.amount_cs),else_=0)),0)).group_by(Vendedor.id,Vendedor.nombre).order_by(Vendedor.nombre).all()
    history_groups=[]
    if history:
        history_groups=query.join(User,User.id==MotoTrip.rider_id).join(Vendedor,Vendedor.id==MotoTrip.seller_id).with_entities(
            User.id.label('rider_id'),func.coalesce(func.nullif(User.full_name,''),User.email).label('rider'),
            Vendedor.id.label('seller_id'),Vendedor.nombre.label('seller'),func.count(MotoTrip.id).label('count'),
            func.coalesce(func.sum(MotoTrip.amount_cs),0).label('registered_total'),
            func.coalesce(func.sum(case((MotoTrip.status=='ANULADO',MotoTrip.amount_cs),else_=0)),0).label('canceled')
        ).group_by(User.id,User.full_name,User.email,Vendedor.id,Vendedor.nombre).order_by(User.full_name,Vendedor.nombre).all()
    context={'request':request,'user':user,'today':today,'trips':trips,'summary':dict(count=count,total=total,registered_total=total+canceled,delivered=delivered,pending=pending,canceled=canceled),'costs':costs,'history_groups':history_groups,'view':view,'history':history,'admin':admin,'can_create':'motorizado' in names,'is_seller':'vendedor' in names,'seller_can':service.seller_can,'page':page,'pages':pages,'start':start,'end':end,'csrf':csrf(request,user),'token':uuid4().hex,'destinations':service.DESTINATIONS,'selected_seller':seller,'selected_rider':rider,'selected_status':status,'sidebar_role_label':'Administrador' if admin else 'Motorizado' if 'motorizado' in names else 'Vendedor'}
    context['close_tokens']={trip.id:uuid4().hex for trip in trips}
    groups={}
    if 'vendedor' in names and user.vendedor_id:
        eligible=query.filter(MotoTrip.seller_id==user.vendedor_id,MotoTrip.status=='RECIBIDO',MotoTrip.closure_id.is_(None)).options(joinedload(MotoTrip.rider)).order_by(MotoTrip.trip_date,MotoTrip.id).limit(100).all()
        for trip in eligible:
            key=(trip.rider_id,trip.branch_id)
            group=groups.setdefault(key,{'rider':trip.rider.full_name or trip.rider.email,'ids':[],'trips':[],'total':0,'token':uuid4().hex})
            group['ids'].append(str(trip.id));group['trips'].append(trip);group['total']+=trip.amount_cs
    context['close_groups']=list(groups.values())
    if request.headers.get('X-Moto-Fragment')=='1':return request.app.state.templates.TemplateResponse('partials/motorizado_results.html',context,headers={'Cache-Control':'no-store'})
    branch,warehouse=web._resolve_branch_bodega(db,user)
    journals=service.bank_journals(db,branch.id) if branch else []
    context.update(sellers=db.query(Vendedor).filter_by(activo=True).order_by(Vendedor.nombre).all(),filter_sellers=db.query(Vendedor).filter(Vendedor.id.in_(service.scope(db,user,db.query(MotoTrip.seller_id)))).order_by(Vendedor.nombre).all() if not admin else db.query(Vendedor).order_by(Vendedor.nombre).all(),riders=db.query(User).join(User.roles).filter(web.Role.name=='motorizado',User.is_active.is_(True)).order_by(User.full_name).all() if admin else [],journals=journals,warehouse=warehouse,transport_ready=bool(db.query(CuentaContable.id).filter_by(codigo='6104',activo=True,tipo='RESULTADO',naturaleza='DEBE').first()))
    return request.app.state.templates.TemplateResponse('motorizado.html',context,headers={'Cache-Control':'no-store'})


@router.post('/motorizado')
def create(request:Request,form=Depends(form_data),db=Depends(get_db),user=Depends(access)):
    verify(request,user,form)
    try:
        prepared=photos.prepare_uploads(form.getlist('photos'))
        trip=service.create(db,user,form);photos.attach(db,user,trip,prepared);db.commit();return redirect(f'Viaje #{trip.id} registrado correctamente.')
    except (ValueError,HTTPException,ArithmeticError) as exc:
        db.rollback()
        if isinstance(exc,HTTPException) and exc.status_code in {401,403,404}:raise
        return redirect(exc,True)


@router.post('/motorizado/{trip_id}/estado')
def transition(trip_id:int,request:Request,form=Depends(form_data),db=Depends(get_db),user=Depends(access)):
    verify(request,user,form)
    try:
        trip=service.transition(db,user,trip_id,str(form.get('action') or ''),str(form.get('reason') or ''),form.get('refund')=='on');db.commit()
        return redirect({'ENTREGADO':'Entrega marcada. Pendiente de confirmar por el vendedor.','RECIBIDO':'Recepción confirmada por el vendedor.','ANULADO':'Viaje anulado correctamente.'}[trip.status])
    except (ValueError,HTTPException,ArithmeticError) as exc:
        db.rollback()
        if isinstance(exc,HTTPException) and exc.status_code in {401,403,404}:raise
        return redirect(exc,True)


@router.post('/motorizado/{trip_id}/fotos')
def upload_photos(trip_id:int,request:Request,form=Depends(form_data),db=Depends(get_db),user=Depends(access)):
    verify(request,user,form)
    try:
        trip=service.allowed_trip(db,user,db.get(MotoTrip,trip_id))
        prepared=photos.prepare_uploads(form.getlist('photos'))
        if not prepared:raise ValueError('Selecciona al menos una fotografía')
        count=photos.attach(db,user,trip,prepared);db.commit();return redirect(f'{count} foto(s) de soporte adjunta(s) al viaje #{trip.id}.',day=trip.trip_date)
    except (ValueError,HTTPException) as exc:
        db.rollback()
        if isinstance(exc,HTTPException):raise
        return redirect(exc,True)


@router.get('/motorizado/fotos/{photo_id}')
def photo(photo_id:int,request:Request,db=Depends(get_db),user=Depends(access)):
    row=db.get(MotoTripPhoto,photo_id)
    if not row:raise HTTPException(404,'Foto no encontrada')
    service.allowed_trip(db,user,db.get(MotoTrip,row.trip_id))
    return Response(row.data,media_type='image/jpeg',headers={'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff','Content-Disposition':f'inline; filename="soporte-{row.id}.jpg"'})


@router.post('/motorizado/cerrar')
def close_trips(request:Request,form=Depends(form_data),db=Depends(get_db),user=Depends(access)):
    verify(request,user,form)
    try:
        values=str(form.get('trip_ids') or '').split(',')
        if len(values)>100 or any(not value.isdigit() for value in values):raise ValueError('Revisa los viajes del cierre')
        closure=service.close_trips(db,user,[int(value) for value in values],str(form.get('operation_key') or ''),form.get('paid')=='on',str(form.get('comment') or ''))
        db.commit();return redirect(f'Cierre #{closure.id}: pago confirmado de C$ {closure.amount_cs:,.2f}. Los viajes quedaron en el historial.')
    except (ValueError,HTTPException,IntegrityError) as exc:
        db.rollback()
        if isinstance(exc,HTTPException) and exc.status_code in {401,403,404}:raise
        return redirect('Este cierre ya fue procesado. Actualiza la vista para consultarlo.' if isinstance(exc,IntegrityError) else exc,True)


def closure_report(request,db,user):
    names=service.roles(user);admin='administrador' in names;today=web.local_today()
    try:
        start=date.fromisoformat(request.query_params.get('from') or str(today));end=date.fromisoformat(request.query_params.get('to') or str(today));page=max(1,int(request.query_params.get('page') or 1))
        if start>end:raise ValueError()
    except ValueError:raise HTTPException(400,'Revisa las fechas del reporte')
    query=service.closure_scope(db,user,db.query(MotoTripClosure)).filter(MotoTripClosure.closure_date.between(start,end))
    seller=request.query_params.get('seller','');rider=request.query_params.get('rider','')
    if seller.isdigit():query=query.filter(MotoTripClosure.seller_id==int(seller))
    if admin and rider.isdigit():query=query.filter(MotoTripClosure.rider_id==int(rider))
    count,total,trip_count=query.with_entities(func.count(MotoTripClosure.id),func.coalesce(func.sum(MotoTripClosure.amount_cs),0),func.coalesce(func.sum(MotoTripClosure.trip_count),0)).one()
    pages=max(1,(count+29)//30);page=min(page,pages)
    rows=query.options(joinedload(MotoTripClosure.rider),joinedload(MotoTripClosure.seller),joinedload(MotoTripClosure.closed_by),selectinload(MotoTripClosure.trips)).order_by(MotoTripClosure.closed_at.desc(),MotoTripClosure.id.desc()).offset((page-1)*30).limit(30).all()
    vendors=db.query(Vendedor).filter(Vendedor.id.in_(service.closure_scope(db,user,db.query(MotoTripClosure.seller_id)))).order_by(Vendedor.nombre).all()
    riders=db.query(User).filter(User.id.in_(service.closure_scope(db,user,db.query(MotoTripClosure.rider_id)))).order_by(User.full_name).all() if admin else []
    return request.app.state.templates.TemplateResponse('motorizado_closures.html',{'request':request,'user':user,'rows':rows,'summary':dict(count=count,total=total,trips=trip_count),'start':start,'end':end,'today':today,'page':page,'pages':pages,'vendors':vendors,'riders':riders,'admin':admin,'selected_seller':seller,'selected_rider':rider,'sidebar_role_label':'Administrador' if admin else 'Motorizado' if 'motorizado' in names else 'Vendedor'},headers={'Cache-Control':'no-store'})
