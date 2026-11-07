"""Administrative preview/confirm correction of an employee's employment start."""
from datetime import date
import re
from uuid import uuid4
from urllib.parse import quote_plus,urlparse
from fastapi import APIRouter,Depends,Request,HTTPException
from fastapi.responses import RedirectResponse
from ..core.deps import get_db
from ..core import payroll_start_date as service
from ..models.payroll import PayrollStartDateChange
from .attendance import _browser_admin
from .payroll import _check_accounting_branch

router=APIRouter()
PATH='/payroll/employees/{ident}/start-date'


def access(request,db,ident):
    from . import web
    if not web._is_pacasholl_company():raise HTTPException(404)
    user=_browser_admin(request,db)
    if not any(r.name=='administrador' for r in user.roles or []):raise HTTPException(403,'Acceso denegado')
    try:profile=service.profile_for(db,ident)
    except ValueError as exc:raise HTTPException(400,str(exc))
    _check_accounting_branch(db,user,profile.employee)
    return user,profile


def render(request,db,user,profile,new_date='',error='',status=200):
    from . import web
    view=None
    if new_date:
        try:view=service.preview(db,profile,date.fromisoformat(new_date),web.local_today())
        except (ValueError,HTTPException) as exc:error=str(getattr(exc,'detail',exc))
    nonce=uuid4().hex
    fingerprint=view['fingerprint'] if view else ''
    signature=service.sign(user,profile.employee_id,new_date,fingerprint,nonce) if view else ''
    history=db.query(PayrollStartDateChange).filter_by(employee_id=profile.employee_id).order_by(PayrollStartDateChange.id.desc()).limit(25).all()
    return request.app.state.templates.TemplateResponse('payroll_start_date.html',dict(request=request,user=user,profile=profile,today=web.local_today(),new_date=new_date,error=error or request.query_params.get('error',''),view=view,nonce=nonce,fingerprint=fingerprint,signature=signature,history=history),status_code=status)


@router.get(PATH)
def page(ident:int,request:Request,db=Depends(get_db)):
    user,profile=access(request,db,ident)
    return render(request,db,user,profile,request.query_params.get('new_date',''))


@router.post(PATH)
async def correct(ident:int,request:Request,db=Depends(get_db)):
    user,profile=access(request,db,ident)
    origin=request.headers.get('origin')
    if origin and urlparse(origin).netloc!=request.url.netloc:raise HTTPException(403,'Origen del formulario inválido')
    form=await request.form(max_fields=7,max_files=0)
    nonce=str(form.get('nonce',''));signature=str(form.get('signature',''));fingerprint=str(form.get('fingerprint',''));new_date=str(form.get('new_date',''))
    if not re.fullmatch(r'[a-f0-9]{32}',nonce) or not re.fullmatch(r'[a-f0-9]{64}',signature) or not re.fullmatch(r'[a-f0-9]{64}',fingerprint):
        raise HTTPException(403,'Abre la comparación antes de confirmar la corrección')
    try:
        if form.get('reviewed')!='on':raise ValueError('Confirma que revisaste la comparación y los períodos afectados')
        result=service.apply(db,user,ident,date.fromisoformat(new_date),form.get('reason',''),nonce,fingerprint,signature)
        db.commit()
    except (ValueError,HTTPException) as exc:
        db.rollback()
        if isinstance(exc,HTTPException) and exc.status_code==403:raise
        return render(request,db,user,profile,new_date,str(getattr(exc,'detail',exc)),400)
    except Exception:
        db.rollback();raise
    return RedirectResponse(f'/payroll/employees/{ident}/start-date?saved={result.id}',303)
