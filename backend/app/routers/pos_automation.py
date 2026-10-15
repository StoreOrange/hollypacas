import base64,hashlib,hmac,json,re,secrets,zipfile
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from fastapi import APIRouter,Depends,Request,HTTPException
from fastapi.responses import Response
from sqlalchemy import func
from ..core.deps import get_db
from ..core import pos_automation as auto
from ..models.pos_automation import PosAutoStation,PosAutoTask,PosPrintJob
from ..models.sales import Preventa,VentaFactura
from ..models.user import User
from . import web

router=APIRouter(tags=['Hollpacas POS automática'])


def company():
    if not web._is_pacasholl_company():raise HTTPException(404)


def browser(request,db,user,station_id):
    company();web._enforce_permission(request,user,'access.sales.registrar')
    branch,bodega=web._resolve_branch_bodega(db,user)
    if not branch or not bodega:raise HTTPException(400,'Selecciona sucursal y bodega de caja')
    station=db.get(PosAutoStation,station_id)
    if not station or station.user_id!=user.id or station.bodega_id!=bodega.id or station.branch_id!=branch.id:raise HTTPException(403,'Esta estación no pertenece a tu usuario y caja')
    return station


def agent(request,db,station_id):
    company();station=db.get(PosAutoStation,station_id)
    token=request.headers.get('authorization','').removeprefix('Bearer ')
    if not station or not station.token_hash or not hmac.compare_digest(hashlib.sha256(token.encode()).hexdigest(),station.token_hash):raise HTTPException(401,'Agente no autorizado')
    user=db.get(User,station.user_id)
    if not user or not user.is_active:raise HTTPException(403,'Usuario de caja inactivo')
    if not web._has_permission(user,'access.sales.registrar'):raise HTTPException(403,'El usuario ya no tiene permiso para facturar')
    allowed=web._user_scoped_branch_ids(db,user)
    if allowed and station.branch_id not in allowed:raise HTTPException(403,'La estación ya no pertenece a la sucursal autorizada')
    return station


def snapshot(db,station):
    now=web.local_now_naive()
    tasks=db.query(PosAutoTask).filter_by(station_id=station.id,status='WAITING').order_by(PosAutoTask.due_at,PosAutoTask.id).limit(20).all()
    tasks+=db.query(PosAutoTask).filter(PosAutoTask.station_id==station.id,PosAutoTask.status!='WAITING').order_by(PosAutoTask.id.desc()).limit(10).all()
    latest=db.query(PosAutoTask).filter(PosAutoTask.station_id==station.id,PosAutoTask.invoice_id.isnot(None)).order_by(PosAutoTask.invoice_id.desc()).first()
    if latest and latest.id not in {task.id for task in tasks}:tasks.append(latest)
    rows=[]
    for task in tasks:
        sale=db.get(Preventa,task.preventa_id);job=db.query(PosPrintJob).filter_by(invoice_id=task.invoice_id).first() if task.invoice_id else None
        invoice=db.get(VentaFactura,task.invoice_id) if task.invoice_id else None
        row={'id':task.id,'preventa_id':task.preventa_id,'preventa':sale.numero if sale else str(task.preventa_id),'status':task.status,'remaining':max(0,int((task.due_at-now).total_seconds()+.999)),'message':task.message or '', 'job_id':job.id if job else None,'print_status':job.status if job else None,'print_message':job.message if job else None,'retry_allowed':bool(job and (job.status in {'FAILED','UNKNOWN'} or (job.status=='DISPATCHED' and job.dispatched_at and now-job.dispatched_at>timedelta(seconds=120))))}
        if invoice:
            row.update(invoice_id=invoice.id,invoice_number=invoice.numero,total=f'{invoice.total_cs:,.2f}',customer=invoice.cliente.nombre if invoice.cliente else 'Consumidor final',items=[{'description':item.producto.descripcion if item.producto else 'Producto','qty':str(item.cantidad),'price':f'{item.precio_unitario_cs:,.2f}'} for item in invoice.items])
        rows.append(row)
    return {'ok':True,'station_id':station.id,'enabled':station.enabled,'online':auto.online(station,now),'printer':station.printer_name or '', 'agent_message':station.agent_message or 'Agente Windows aún no conectado','tasks':rows}


@router.post('/sales/auto-preventas/station')
async def station_create(request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    company();web._enforce_permission(request,user,'access.sales.registrar')
    payload=await request.json();ident=str(payload.get('station_id') or '')
    if not re.fullmatch('[0-9a-f]{32}',ident):raise HTTPException(400,'Identificador de caja inválido')
    branch,bodega=web._resolve_branch_bodega(db,user)
    if not branch or not bodega:raise HTTPException(400,'Selecciona sucursal y bodega de caja')
    web._lock_accounting_transaction(db)
    station=db.get(PosAutoStation,ident)
    if station:station=browser(request,db,user,ident)
    else:
        station=PosAutoStation(id=ident,user_id=user.id,branch_id=branch.id,bodega_id=bodega.id,enabled=False);db.add(station);db.flush()
    db.commit();return snapshot(db,station)


@router.post('/sales/auto-preventas/{station_id}/enable')
async def enable(station_id:str,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    station=browser(request,db,user,station_id);payload=await request.json()
    if not isinstance(payload.get('enabled'),bool):raise HTTPException(400,'Indica si deseas activar la automatización')
    try:auto.set_enabled(db,station,payload['enabled'],web.local_now_naive());db.commit()
    except ValueError as exc:db.rollback();raise HTTPException(409,str(exc))
    return snapshot(db,station)


@router.post('/sales/auto-preventas/{station_id}/tick')
async def tick(station_id:str,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    station=browser(request,db,user,station_id);payload=await request.json()
    auto.lock_scope(db,station);now=web.local_now_naive();station.last_tick=now
    tasks=auto.prepare(db,station,now)
    db.commit()  # La recepción y el plazo sobreviven a una validación rechazada al facturar.
    if station.enabled and auto.online(station,now) and payload.get('busy') is False:
        due=next((task for task in tasks if task.due_at<=now),None)
        if due:
            task_id=due.id
            try:
                auto.lock_scope(db,station)
                due=db.query(PosAutoTask).filter_by(id=task_id).populate_existing().first()
                if due.status=='WAITING':await auto.invoice_due(db,station,due,request,user,web.local_now_naive())
                db.commit()
            except (ValueError,HTTPException) as exc:
                db.rollback();due=db.get(PosAutoTask,task_id)
                if due and due.status=='WAITING':due.status='ERROR';due.message=str(getattr(exc,'detail',exc))[:400];db.commit()
    return snapshot(db,station)


@router.post('/sales/auto-preventas/{station_id}/installer')
def installer(station_id:str,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    station=browser(request,db,user,station_id);auto.lock_scope(db,station)
    if station.enabled:raise HTTPException(409,'Desactiva la facturación automática antes de volver a vincular el agente')
    token=secrets.token_urlsafe(40);station.token_hash=hashlib.sha256(token.encode()).hexdigest();station.agent_ready=False;station.agent_seen=None
    config={'api_url':str(request.base_url).rstrip('/'),'station_id':station.id,'token':token}
    root=Path(__file__).resolve().parents[2];buffer=BytesIO()
    with zipfile.ZipFile(buffer,'w',zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr('hollpacas-pos.json',json.dumps(config,indent=2))
        bundle.write(root/'scripts'/'hollpacas_pos_agent.py','hollpacas_pos_agent.py')
        bundle.write(root/'deploy'/'install_pos_agent_windows.ps1','Instalar.ps1')
        bundle.write(root/'docs'/'agente-pos-windows.txt','LEEME.txt')
    db.commit()
    return Response(buffer.getvalue(),media_type='application/zip',headers={'Content-Disposition':'attachment; filename="Hollpacas-POS-Windows.zip"','Cache-Control':'no-store'})


@router.post('/sales/auto-preventas/{station_id}/jobs/{job_id}/retry')
async def retry(station_id:str,job_id:int,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    station=browser(request,db,user,station_id);auto.lock_scope(db,station)
    payload=await request.json()
    if payload.get('confirmed') is not True:raise HTTPException(400,'Revisa la cola de Windows y confirma la reimpresión')
    job=db.query(PosPrintJob).filter_by(id=job_id,station_id=station.id).with_for_update().first()
    if not job:raise HTTPException(404)
    allowed=job.status in {'FAILED','UNKNOWN'} or (job.status=='DISPATCHED' and job.dispatched_at and web.local_now_naive()-job.dispatched_at>timedelta(seconds=120))
    if not allowed:raise HTTPException(409,'Este trabajo ya fue enviado o está en curso')
    job.status='QUEUED';job.claim_id=None;job.message='Reimpresión solicitada por '+(user.full_name or user.email);db.commit()
    return snapshot(db,station)


@router.get('/sales/auto-preventas/{station_id}/jobs/{job_id}/ticket')
def ticket(station_id:str,job_id:int,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    station=browser(request,db,user,station_id);job=db.query(PosPrintJob).filter_by(id=job_id,station_id=station.id).first()
    if not job:raise HTTPException(404)
    invoice=db.get(VentaFactura,job.invoice_id)
    return Response(web._build_pos_ticket_pdf_bytes(invoice,web._company_profile_payload(db)),media_type='application/pdf',headers={'Cache-Control':'no-store'})


@router.post('/api/pos-agent/{station_id}/next')
async def next_job(station_id:str,request:Request,db=Depends(get_db)):
    station=agent(request,db,station_id);auto.lock_scope(db,station);payload=await request.json()
    station.agent_seen=web.local_now_naive();station.agent_ready=payload.get('ready') is True;station.printer_name=str(payload.get('printer') or '')[:200];station.agent_message=str(payload.get('message') or '')[:300]
    job=db.query(PosPrintJob).filter_by(station_id=station.id,status='QUEUED').order_by(PosPrintJob.id).with_for_update().first() if station.agent_ready else None
    result={'ok':True,'job':None}
    if job:
        invoice=db.get(VentaFactura,job.invoice_id)
        if not invoice or invoice.bodega_id!=station.bodega_id:raise HTTPException(409,'La factura no pertenece a esta caja')
        pdf=web._build_pos_ticket_pdf_bytes(invoice,web._company_profile_payload(db))
        job.status='DISPATCHED';job.claim_id=secrets.token_hex(16);job.dispatched_at=web.local_now_naive();job.message='Recibido por el agente; confirmación pendiente'
        result['job']={'id':job.id,'claim_id':job.claim_id,'copies':job.copies,'pdf':base64.b64encode(pdf).decode(),'sha256':hashlib.sha256(pdf).hexdigest()}
    db.commit();return result


@router.post('/api/pos-agent/{station_id}/jobs/{job_id}/ack')
async def ack(station_id:str,job_id:int,request:Request,db=Depends(get_db)):
    station=agent(request,db,station_id);payload=await request.json();auto.lock_scope(db,station)
    job=db.query(PosPrintJob).filter_by(id=job_id,station_id=station.id).with_for_update().first()
    if not job or not job.claim_id or not hmac.compare_digest(job.claim_id,str(payload.get('claim_id') or '')):raise HTTPException(409,'Confirmación de otro intento de impresión')
    status=payload.get('status')
    if status not in {'SENT','FAILED','UNKNOWN'}:raise HTTPException(400,'Estado de impresión inválido')
    if job.status=='DISPATCHED':job.status=status;job.message=str(payload.get('message') or '')[:400];job.completed_at=web.local_now_naive()
    db.commit();return {'ok':True}


@router.post('/sales/auto-preventas/{station_id}/revoke')
def revoke(station_id:str,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    station=browser(request,db,user,station_id);auto.set_enabled(db,station,False,web.local_now_naive())
    station.token_hash=None;station.agent_ready=False;station.agent_seen=None;station.agent_message='Agente revocado por el usuario de caja'
    db.commit();return snapshot(db,station)


@router.post('/sales/auto-preventas/{station_id}/tasks/{task_id}/cancel')
def cancel_task(station_id:str,task_id:int,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    station=browser(request,db,user,station_id);auto.lock_scope(db,station)
    task=db.query(PosAutoTask).filter_by(id=task_id,station_id=station.id,status='WAITING').with_for_update().first()
    if not task:raise HTTPException(409,'La preventa ya no está en espera')
    task.status='CANCELLED';task.message='Retirada de automatización por el usuario; sigue disponible para facturación manual'
    db.commit();return snapshot(db,station)
