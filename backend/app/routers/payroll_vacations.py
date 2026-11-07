from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4
from urllib.parse import quote_plus
from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import RedirectResponse, Response
from sqlalchemy import or_
from ..core.deps import get_db
from ..core import payroll_benefits as benefits, accounting_payroll as ledger
from ..models.payroll import PayrollEmployeeProfile, PayrollVacationMovement, PayrollBenefitSetting, PayrollBenefitAccrual, PayrollSettlement, PayrollVacationDraftAdjustment
from ..models.sales import CuentaContable, AccountingJournal
from ..models.inventory import Bodega
from ..models.attendance import HREmployee
from .attendance import _browser_admin
from .payroll import _check_accounting_branch

router=APIRouter(tags=['Payroll vacations'])


def access(request,db):
    from . import web
    user=_browser_admin(request,db)
    if not web._is_pacasholl_company():raise HTTPException(404)
    return user


def response(message,error=False):
    return RedirectResponse('/payroll/vacations?'+('error=' if error else 'ok=')+quote_plus(str(getattr(message,'detail',message))),status_code=303)


@router.get('/payroll/vacations')
def home(request:Request, employee_id:str='', db=Depends(get_db)):
    from . import web
    user=access(request,db);today=web.local_today();scope=web._user_scoped_branch_ids(db,user)
    profiles=db.query(PayrollEmployeeProfile).filter_by(active=True).all()
    profiles=[p for p in profiles if p.employee.status=='ACTIVE' and (not scope or p.employee.branch_id in scope)]
    rows=[]
    for profile in sorted(profiles,key=lambda p:p.employee.full_name):
        try:values=benefits.balance(db,profile,today);error=None
        except ValueError as exc:values={'earned':0,'used':0,'days':0,'amount':0};error=str(exc)
        last=db.query(PayrollBenefitAccrual).filter_by(employee_id=profile.employee_id).order_by(PayrollBenefitAccrual.as_of.desc()).first()
        rows.append({'profile':profile,**values,'last':last,'error':error})
    ids=[p.employee_id for p in profiles]
    drafts={r.employee_id:r for r in db.query(PayrollSettlement).filter(PayrollSettlement.employee_id.in_(ids),PayrollSettlement.status=='DRAFT').all()}
    adjustments={}
    adjustment_query=db.query(PayrollVacationDraftAdjustment).join(PayrollVacationMovement,PayrollVacationMovement.id==PayrollVacationDraftAdjustment.movement_id).join(HREmployee,HREmployee.id==PayrollVacationMovement.employee_id)
    if scope:adjustment_query=adjustment_query.filter(HREmployee.branch_id.in_(scope))
    for adjustment in adjustment_query.all():
        adjustments.setdefault(adjustment.movement_id,[]).append(adjustment.settlement_id)
    history=db.query(PayrollVacationMovement).join(HREmployee, HREmployee.id==PayrollVacationMovement.employee_id)
    if scope:history=history.filter(HREmployee.branch_id.in_(scope))
    if employee_id.isdigit():history=history.filter(PayrollVacationMovement.employee_id==int(employee_id))
    defaults={}
    for field,code,name in [('expense_id','6209','Prestaciones Laborales'),('vacation_payable_id','21131','Vacaciones por Pagar'),('bonus_payable_id','21132','Aguinaldo por Pagar'),('seniority_payable_id','21133','Provisión de Indemnización')]:
        account=db.query(CuentaContable).filter_by(codigo=code,nombre=name,activo=True).first()
        if account:defaults[field]=account.id
    return request.app.state.templates.TemplateResponse('payroll_vacations.html',{'request':request,'user':user,'rows':rows,'today':today,'config':db.get(PayrollBenefitSetting,1),'accounts':db.query(CuentaContable).filter_by(activo=True).order_by(CuentaContable.codigo).all(),'journals':db.query(AccountingJournal).filter(AccountingJournal.active.is_(True),AccountingJournal.kind.in_(['CASH','BANK'])).all(),'warehouses':db.query(Bodega).filter(Bodega.activo.is_(True),Bodega.branch_id.in_(scope)).all() if scope else db.query(Bodega).filter(Bodega.activo.is_(True)).all(),'history':history.order_by(PayrollVacationMovement.created_at.desc()).limit(100).all(),'token':uuid4().hex,'defaults':defaults,'drafts':drafts,'draft_adjustments':adjustments})


@router.post('/payroll/vacations/config')
async def configure(request:Request,db=Depends(get_db)):
    from . import web
    user=access(request,db);web._enforce_permission(request,user,'access.accounting.financial_data');form=await request.form();ledger.lock(db)
    try:
        setting=db.get(PayrollBenefitSetting,1)
        if db.query(PayrollBenefitAccrual.id).first():raise ValueError('Las cuentas de un control iniciado se mantienen para preservar su conciliación; los cambios requieren reclasificación contable')
        if form.get('review_opening')!='on':raise ValueError('Confirma la revisión del saldo inicial para no duplicar obligaciones anteriores')
        start=date.fromisoformat(str(form.get('start_date')))
        if start!=web.local_today():raise ValueError('Inicia desde hoy; el saldo acumulado se incorpora en esta fecha sin alterar cierres anteriores')
        if not setting:setting=PayrollBenefitSetting(id=1,start_date=start);db.add(setting)
        for field in ('expense_id','opening_debit_id','vacation_payable_id','bonus_payable_id','seniority_payable_id'):setattr(setting,field,int(form.get(field) or 0))
        setting.enabled=True;setting.start_date=start;setting.updated_by=user.email;db.flush();benefits.config(db)
        db.commit();return response('Provisión diaria activada. El sistema incorporará el saldo acumulado con la contrapartida inicial elegida')
    except (ValueError,HTTPException) as exc:db.rollback();return response(exc,True)


@router.post('/payroll/vacations/record')
async def record(request:Request,db=Depends(get_db)):
    from . import web
    user=access(request,db);form=await request.form();ledger.lock(db)
    try:
        profile=db.query(PayrollEmployeeProfile).filter_by(employee_id=int(form.get('employee_id') or 0),active=True).with_for_update().first()
        if not profile or profile.employee.status!='ACTIVE':raise ValueError('Selecciona un trabajador activo')
        _check_accounting_branch(db,user,profile.employee)
        warehouse=db.get(Bodega,int(form.get('cash_bodega_id') or 0))
        move=benefits.record(db,profile,str(form.get('kind')),web.local_today(),Decimal(str(form.get('days') or 0)),date.fromisoformat(str(form.get('date_from'))) if form.get('date_from') else None,date.fromisoformat(str(form.get('date_to'))) if form.get('date_to') else None,str(form.get('reason') or ''),str(form.get('operation_key') or ''),user.email,str(form.get('payment_method') or 'EFECTIVO'),int(form.get('journal_id') or 0),warehouse,update_settlement=form.get('update_settlement')=='on')
        adjusted=db.query(PayrollVacationDraftAdjustment).filter_by(movement_id=move.id).count()
        db.commit()
        return response(('Pago de vacaciones registrado y contabilizado. '+('La liquidación en borrador se actualizó para no pagar esas vacaciones dos veces.' if adjusted else '')) if move.kind=='CASH' else 'Descanso registrado; su salario se pagará en la planilla habitual')
    except (ValueError,HTTPException,ArithmeticError) as exc:db.rollback();return response(exc,True)


@router.post('/payroll/vacations/sync')
def sync(request:Request,db=Depends(get_db)):
    user=access(request,db)
    from . import web
    web._enforce_permission(request,user,'access.accounting.financial_data')
    # Usa la misma rutina idempotente de la ejecución automática.
    current=db.get(PayrollBenefitSetting,1)
    if not current or not current.enabled:return response('Primero configura y activa la provisión de prestaciones',True)
    benefits.run_daily()
    db.expire_all();setting=db.get(PayrollBenefitSetting,1)
    return response(setting.last_error if setting and setting.last_error else 'Provisión diaria actualizada',bool(setting and setting.last_error))


@router.post('/payroll/vacations/{movement_id}/void')
def void(movement_id:int,request:Request,reason:str=Form(...),db=Depends(get_db)):
    user=access(request,db);ledger.lock(db)
    try:
        move=db.query(PayrollVacationMovement).filter_by(id=movement_id).with_for_update().first()
        if not move:raise ValueError('No se encontró el movimiento')
        _check_accounting_branch(db,user,move.employee)
        benefits.void_movement(db,move,reason,user.email)
        db.commit();return response('Movimiento anulado y saldo restaurado. El pago anulado registra el reintegro correspondiente')
    except (ValueError,HTTPException) as exc:db.rollback();return response(exc,True)


def _pdf_response(db,moves,individual=False):
    from . import web
    from ..models.sales import AccountingEntry,ReciboCaja
    from ..models.payroll import PayrollVacationReversal
    from ..core.payroll_vacation_pdf import build
    ids=[m.id for m in moves]
    entries={e.id:e for e in db.query(AccountingEntry).filter(AccountingEntry.id.in_([m.accounting_entry_id for m in moves if m.accounting_entry_id])).all()} if individual else {}
    receipts={r.id:r for r in db.query(ReciboCaja).filter(ReciboCaja.id.in_([m.cash_receipt_id for m in moves if m.cash_receipt_id])).all()} if individual else {}
    adjustments={}
    if individual:
        for adjustment in db.query(PayrollVacationDraftAdjustment).filter(PayrollVacationDraftAdjustment.movement_id.in_(ids)).all():adjustments.setdefault(adjustment.movement_id,[]).append(adjustment)
    reversals={r.movement_id:r for r in db.query(PayrollVacationReversal).filter(PayrollVacationReversal.movement_id.in_(ids)).all()} if individual else {}
    data=build(moves,web._company_profile_payload(db),web.local_now_naive(),entries,receipts,adjustments,reversals,individual)
    filename=f'vacaciones_VAC-{moves[0].id:06d}.pdf' if individual else 'historial_vacaciones.pdf'
    return Response(data,media_type='application/pdf',headers={'Content-Disposition':f'inline; filename="{filename}"','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})


@router.get('/payroll/vacations/history.pdf')
def history_pdf(request:Request,employee_id:str='',db=Depends(get_db)):
    from . import web
    user=access(request,db);scope=web._user_scoped_branch_ids(db,user)
    query=db.query(PayrollVacationMovement).join(HREmployee,HREmployee.id==PayrollVacationMovement.employee_id)
    if scope:query=query.filter(HREmployee.branch_id.in_(scope))
    if employee_id:
        if not employee_id.isdigit():raise HTTPException(400,'Trabajador inválido')
        query=query.filter(PayrollVacationMovement.employee_id==int(employee_id))
    moves=query.order_by(PayrollVacationMovement.effective_date.desc(),PayrollVacationMovement.id.desc()).limit(1001).all()
    if len(moves)>1000:raise HTTPException(400,'Filtra por trabajador para generar un reporte de hasta 1,000 movimientos')
    return _pdf_response(db,moves)


@router.get('/payroll/vacations/{movement_id}/pdf')
def movement_pdf(movement_id:int,request:Request,db=Depends(get_db)):
    user=access(request,db)
    move=db.get(PayrollVacationMovement,movement_id)
    if not move:raise HTTPException(404,'Movimiento de vacaciones no encontrado')
    _check_accounting_branch(db,user,move.employee)
    return _pdf_response(db,[move],individual=True)
