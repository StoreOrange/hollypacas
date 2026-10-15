"""Hollpacas: temporizador optativo e impresión nativa, sin endpoints de agente."""
from datetime import timedelta
import secrets
import asyncio
from fastapi import APIRouter,Depends,Request,HTTPException
from ..core.deps import get_db
from ..core import pos_automation as auto
from ..models.pos_automation import PosAutoStation,PosAutoTask,PosPrintJob
from ..models.sales import VentaFactura,Preventa
from . import web
from .pos_automation import browser,station_create,snapshot

router=APIRouter(prefix='/sales/automatico-pos',tags=['POS navegador Hollpacas'])


def state(db,station):
    data=snapshot(db,station)
    # La interfaz no consume ni presenta configuración del agente retirado.
    completed=[]
    pending=[]
    for task in data['tasks']:
        sale=db.get(Preventa,task['preventa_id'])
        invoiced=bool(sale and (sale.venta_factura_id or sale.estado=='FACTURADA'))
        if invoiced:completed.append(task['preventa_id'])
        if task['print_status']=='SENT' or task['status']=='CANCELLED':continue
        if invoiced and not task['job_id']:continue
        if not sale or sale.estado=='ANULADA':continue
        pending.append(task)
    return {'ok':True,'station_id':station.id,'enabled':station.enabled,'tasks':pending,'completed_preventa_ids':completed}


async def station(request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    result=await station_create(request,db,user)
    return state(db,db.get(PosAutoStation,result['station_id']))


async def enable(station_id:str,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    box=browser(request,db,user,station_id);payload=await request.json()
    if not isinstance(payload.get('enabled'),bool):raise HTTPException(400,'Indica activar o desactivar')
    try:auto.set_enabled(db,box,payload['enabled'],web.local_now_naive(),require_agent=False);db.commit()
    except ValueError as exc:db.rollback();raise HTTPException(409,str(exc))
    return state(db,box)


async def tick(station_id:str,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    box=browser(request,db,user,station_id);payload=await request.json()
    auto.lock_scope(db,box);now=web.local_now_naive();box.last_tick=now
    tasks=auto.prepare(db,box,now);db.commit()
    # Una solicitud sin confirmación requiere revisión; no acumular nuevas facturas sin imprimir.
    pending=db.query(PosPrintJob.id).filter(PosPrintJob.station_id==box.id,PosPrintJob.status.in_(['QUEUED','DISPATCHED','UNKNOWN'])).first()
    if box.enabled and not pending and payload.get('busy') is False:
        due=next((task for task in tasks if task.due_at<=now),None)
        if due:
            ident=due.id
            try:
                auto.lock_scope(db,box)
                due=db.query(PosAutoTask).filter_by(id=ident).populate_existing().first()
                if due.status=='WAITING':await auto.invoice_due(db,box,due,request,user,web.local_now_naive(),require_agent=False)
                db.commit()
            except (ValueError,HTTPException) as exc:
                db.rollback();due=db.get(PosAutoTask,ident)
                if due and due.status=='WAITING':due.status='ERROR';due.message=str(getattr(exc,'detail',exc))[:400];db.commit()
    return state(db,box)


@router.post('/{station_id}/claim')
def claim(station_id:str,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    box=browser(request,db,user,station_id);auto.lock_scope(db,box)
    if db.query(PosPrintJob.id).filter_by(station_id=box.id,status='DISPATCHED').first():
        db.commit()
        return {'ok':True,'job':None}
    job=db.query(PosPrintJob).filter_by(station_id=box.id,status='QUEUED').order_by(PosPrintJob.id).with_for_update().first()
    if not job:
        db.commit()
        return {'ok':True,'job':None}
    job.status='DISPATCHED';job.claim_id=secrets.token_hex(16);job.dispatched_at=web.local_now_naive();job.message='Impresión solicitada al navegador'
    db.commit()
    return {'ok':True,'job':{'id':job.id,'claim_id':job.claim_id,'url':f'/sales/automatico-pos/{box.id}/jobs/{job.id}/ticket?claim={job.claim_id}'}}


@router.get('/{station_id}/jobs/{job_id}/ticket')
def ticket(station_id:str,job_id:int,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    box=browser(request,db,user,station_id)
    job=db.query(PosPrintJob).filter_by(id=job_id,station_id=box.id,status='DISPATCHED').first()
    if not job or not job.claim_id or not secrets.compare_digest(job.claim_id,request.query_params.get('claim','')):raise HTTPException(409,'Intento de impresión no vigente')
    invoice=db.get(VentaFactura,job.invoice_id)
    if not invoice or invoice.bodega_id!=box.bodega_id:raise HTTPException(404)
    response=web.sales_ticket_print(request,invoice.id,db,user)
    context=dict(response.context)
    context.update(browser_auto_job={'id':job.id,'claim_id':job.claim_id},browser_auto_copies=max(1,min(5,job.copies)))
    result=request.app.state.templates.TemplateResponse('sales_ticket_print.html',context)
    result.headers['Cache-Control']='no-store'
    return result


async def ack(station_id:str,job_id:int,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    box=browser(request,db,user,station_id);payload=await request.json();auto.lock_scope(db,box)
    job=db.query(PosPrintJob).filter_by(id=job_id,station_id=box.id).with_for_update().first()
    if not job or not job.claim_id or not secrets.compare_digest(job.claim_id,str(payload.get('claim_id') or '')):raise HTTPException(409,'Confirmación de otro intento')
    status=payload.get('status')
    if status not in {'SENT','UNKNOWN'}:raise HTTPException(400)
    if job.status=='DISPATCHED':
        job.status=status;job.completed_at=web.local_now_naive();job.message='Diálogo/ciclo de impresión finalizado; verifica salida en Epson' if status=='SENT' else 'Sin confirmación del navegador; revisa la Epson antes de reimprimir'
    db.commit();return state(db,box)


async def retry(station_id:str,job_id:int,request:Request,db=Depends(get_db),user=Depends(web._require_admin_web)):
    box=browser(request,db,user,station_id);payload=await request.json();auto.lock_scope(db,box)
    if payload.get('confirmed') is not True:raise HTTPException(400,'Confirma tras revisar la impresora')
    job=db.query(PosPrintJob).filter_by(id=job_id,station_id=box.id).with_for_update().first()
    if not job:raise HTTPException(404)
    if job.status=='QUEUED' or (job.status=='DISPATCHED' and job.dispatched_at and web.local_now_naive()-job.dispatched_at<timedelta(seconds=120)):raise HTTPException(409,'Impresión en curso')
    job.status='QUEUED';job.claim_id=None;job.message='Reimpresión manual de la misma factura';db.commit()
    return state(db,box)


# Leer el cuerpo en el loop; ejecutar DB y bloqueos síncronos en el pool de hilos.
# Esperar pg_advisory_xact_lock dentro del loop detenía también login y navegación.
async def _json_body(request:Request):
    return await request.json()


@router.post('/station')
def station_route(request:Request,payload=Depends(_json_body),db=Depends(get_db),user=Depends(web._require_admin_web)):
    return asyncio.run(station(request,db,user))


@router.post('/{station_id}/enable')
def enable_route(station_id:str,request:Request,payload=Depends(_json_body),db=Depends(get_db),user=Depends(web._require_admin_web)):
    return asyncio.run(enable(station_id,request,db,user))


@router.post('/{station_id}/tick')
def tick_route(station_id:str,request:Request,payload=Depends(_json_body),db=Depends(get_db),user=Depends(web._require_admin_web)):
    return asyncio.run(tick(station_id,request,db,user))


@router.post('/{station_id}/jobs/{job_id}/ack')
def ack_route(station_id:str,job_id:int,request:Request,payload=Depends(_json_body),db=Depends(get_db),user=Depends(web._require_admin_web)):
    return asyncio.run(ack(station_id,job_id,request,db,user))


@router.post('/{station_id}/jobs/{job_id}/retry')
def retry_route(station_id:str,job_id:int,request:Request,payload=Depends(_json_body),db=Depends(get_db),user=Depends(web._require_admin_web)):
    return asyncio.run(retry(station_id,job_id,request,db,user))
