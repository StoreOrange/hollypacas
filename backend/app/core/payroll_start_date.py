"""Reviewed hire-date correction; preserve paid history and post only current deltas."""
import hashlib
import hmac
import json
import uuid
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from sqlalchemy import func
from fastapi import HTTPException
from . import payroll_benefits as b, accounting_payroll as ledger
from ..models.payroll import (PayrollEmployeeProfile,PayrollStartDateChange,PayrollVacationMovement,PayrollSettlement,PayrollPeriod,PayrollCalculation,PayrollPayment,PayrollBenefitSetting,PayrollBenefitAccrual)
from ..models.attendance import HREmployee


def profile_for(db,ident,lock=False):
    q=db.query(PayrollEmployeeProfile).filter_by(employee_id=ident)
    if lock:q=q.populate_existing().with_for_update()
    profile=q.first()
    if not profile:raise ValueError('El empleado necesita un perfil laboral antes de corregir su fecha')
    if lock:db.refresh(profile.employee,with_for_update=True)
    if not profile.active or profile.employee.status!='ACTIVE':raise ValueError('Esta corrección se realiza sobre trabajadores activos; un empleado liquidado requiere revisar su liquidación')
    return profile


def candidate(profile,new_date):
    employee=SimpleNamespace(hire_date=new_date,full_name=profile.employee.full_name,branch_id=profile.employee.branch_id)
    return SimpleNamespace(employee_id=profile.employee_id,employee=employee,contract_start=new_date,monthly_salary=profile.monthly_salary,vacation_paid_through=profile.vacation_paid_through,bonus_paid_through=profile.bonus_paid_through,seniority_paid_through=profile.seniority_paid_through,currency=profile.currency)


def figures(db,profile,today,strict=True):
    from ..routers.payroll import _benefit_preview
    result=_benefit_preview(profile,profile.employee,today)
    try:
        vacation=b.balance(db,profile,today)
        result['vacation_days']=vacation['days'];result['vacation_amount']=vacation['amount']
    except ValueError as exc:
        if strict:raise
        result['vacation_days']=result['vacation_amount']=None;result['balance_error']=str(exc)
    return result


def settlement_values(db,row,profile):
    values=figures(db,profile,row.termination_date)
    if row.termination_date<profile.contract_start:raise ValueError('La nueva fecha es posterior al cese de una liquidación en borrador. Revisa o anula primero ese borrador')
    days=max(Decimal(30),values['seniority_days']) if row.apply_seniority and values['service_days']>0 else Decimal(0)
    values['seniority_days']=days;values['seniority_amount']=b.money(Decimal(profile.monthly_salary)/30*days)
    gross=b.money(values['vacation_amount']+values['bonus_amount']+values['seniority_amount']+Decimal(row.other_additions))
    if Decimal(row.deductions_amount)>gross-values['bonus_amount']:
        raise ValueError('La corrección deja descuentos excesivos en una liquidación en borrador. Ajusta o anula primero ese borrador')
    values.update(gross_amount=gross,net_amount=b.money(gross-Decimal(row.deductions_amount)),salary_snapshot=b.money(profile.monthly_salary))
    return values


def preview(db,profile,new_date,today):
    if not isinstance(new_date,date) or new_date<date(1900,1,1) or new_date>today:
        raise ValueError('Indica una fecha de ingreso válida, desde 1900 y no posterior a hoy')
    if new_date==(profile.contract_start or profile.employee.hire_date) and profile.contract_start==new_date and profile.employee.hire_date==new_date:
        raise ValueError('La fecha indicada ya está registrada en el perfil y en la ficha del empleado')
    if profile.currency!='NIO' or not Decimal(profile.monthly_salary).is_finite() or Decimal(profile.monthly_salary)<=0:
        raise ValueError('Revisa primero el salario mensual positivo en córdobas del perfil laboral')
    changed=candidate(profile,new_date)
    before=figures(db,profile,today,strict=False)
    after=figures(db,changed,today)
    movements=db.query(PayrollVacationMovement).filter_by(employee_id=profile.employee_id,status='ACTIVE').order_by(PayrollVacationMovement.id).all()
    # Protect leave already consumed/reserved at each original entitlement cutoff.
    for cutoff in sorted({m.effective_date for m in movements if m.kind!='SETTLEMENT'}):
        try:b.balance(db,changed,cutoff)
        except ValueError:raise ValueError('La nueva fecha deja vacaciones utilizadas sin días acumulados al '+cutoff.strftime('%d/%m/%Y')+'. Revisa primero ese historial; no se descontarán pagos automáticamente')
    settlements=db.query(PayrollSettlement).filter_by(employee_id=profile.employee_id).order_by(PayrollSettlement.id).all()
    draft_settlements=[(row,settlement_values(db,row,changed)) for row in settlements if row.status=='DRAFT']
    calculations=db.query(PayrollCalculation).join(PayrollPeriod).filter(PayrollCalculation.employee_id==profile.employee_id).order_by(PayrollCalculation.id).all()
    periods=[];protected=[]
    for calc in calculations:
        period=calc.period
        paid=db.query(PayrollPayment.id).filter_by(period_id=period.id,status='PAID').first()
        if period.status=='DRAFT' and not period.accounting_entry_id and not paid:periods.append(period)
        else:protected.append(period.id)
    setting=db.get(PayrollBenefitSetting,1)
    latest=db.query(PayrollBenefitAccrual).filter_by(employee_id=profile.employee_id).order_by(PayrollBenefitAccrual.as_of.desc()).first()
    if latest and (not setting or not setting.enabled):
        raise ValueError('El empleado tiene prestaciones contabilizadas con provisión desactivada. Revisa la configuración antes de corregir')
    if latest and latest.as_of>today:raise ValueError('Existe una provisión futura. Regulariza su fecha antes de corregir el ingreso')
    accounting=bool(setting and setting.enabled)
    old_amounts=[Decimal(getattr(latest,k)) for k in ('vacation_amount','bonus_amount','seniority_amount')] if latest else [Decimal(0)]*3
    expected=b.targets(db,changed,today) if accounting else []
    if accounting:
        b.config(db)
        if today<setting.start_date:raise ValueError('La provisión comienza después de hoy; revisa su configuración')
        if expected!=old_amounts:
            from ..routers import web
            web._assert_accounting_date_open(db,today)
    result=dict(before=before,after=after,periods=periods,protected=protected,draft_settlements=draft_settlements,paid_settlements=sum(r.status=='PAID' for r in settlements),accounting=accounting,old_amounts=old_amounts,expected=expected,differences=[b.money(a-v) for a,v in zip(expected,old_amounts)])
    # Sign all inputs which can change the reviewed result, including paid cutoffs and ledgers.
    signature=dict(employee=profile.employee_id,branch=profile.employee.branch_id,old=[profile.contract_start,profile.employee.hire_date],salary=profile.monthly_salary,new=new_date,today=today,cutoffs=[profile.vacation_paid_through,profile.bonus_paid_through,profile.seniority_paid_through],before=before,after=after,accounting=accounting,expected=expected,accounts=[getattr(setting,k) for k in ('start_date','expense_id','opening_debit_id','vacation_payable_id','bonus_payable_id','seniority_payable_id')] if setting else None,latest=[latest.id,latest.as_of,latest.accounting_entry_id,*old_amounts] if latest else None,movements=[[m.id,m.kind,m.status,m.effective_date,m.date_from,m.date_to,m.days,m.amount] for m in movements],settlements=[[r.id,r.status,r.termination_date,r.other_additions,r.deductions_amount,r.apply_seniority,r.salary_snapshot] for r in settlements],calculations=[[c.id,c.period_id,c.period.status,c.period.accounting_entry_id,c.calculated_at,c.base_pay,c.net_pay] for c in calculations],protected=protected)
    result['fingerprint']=hashlib.sha256(json.dumps(signature,sort_keys=True,default=str).encode()).hexdigest()
    return result


def sign(user,ident,new_date,fingerprint,nonce):
    from ..routers import web
    payload=f'payroll-start:{user.id}:{ident}:{new_date}:{fingerprint}:{nonce}'
    return hmac.new(web.SECRET_KEY.encode(),payload.encode(),hashlib.sha256).hexdigest()


def apply(db,user,ident,new_date,reason,nonce,fingerprint,signature):
    from ..routers import web,payroll
    if not web._is_pacasholl_company():raise HTTPException(404)
    if not any(r.name=='administrador' for r in user.roles or []):raise HTTPException(403,'Solo el administrador puede corregir fechas laborales')
    ledger.lock(db)
    profile=profile_for(db,ident,lock=True)
    payroll._check_accounting_branch(db,user,profile.employee)
    existing=db.query(PayrollStartDateChange).filter_by(operation_key=nonce).first()
    if existing:
        if existing.employee_id!=ident or existing.changed_by_id!=user.id:raise HTTPException(403,'Operación inválida')
        if not hmac.compare_digest(signature,sign(user,ident,new_date,fingerprint,nonce)):raise HTTPException(403,'Confirmación inválida')
        return existing
    reason=str(reason or '').strip()
    if not 10<=len(reason)<=1000:raise ValueError('Describe el motivo de la corrección entre 10 y 1,000 caracteres')
    view=preview(db,profile,new_date,web.local_today())
    if fingerprint!=view['fingerprint'] or not hmac.compare_digest(signature,sign(user,ident,new_date,fingerprint,nonce)):
        raise ValueError('Los datos cambiaron desde la revisión. Vuelve a comparar los cálculos antes de guardar')
    audit=PayrollStartDateChange(operation_key=nonce,employee_id=ident,old_contract_start=profile.contract_start,old_hire_date=profile.employee.hire_date,new_start=new_date,as_of=web.local_today(),reason=reason,changed_by_id=user.id,changed_by=user.email,created_at=web.local_now_naive(),before_json=json.dumps(view['before'],default=str),after_json=json.dumps(view['after'],default=str))
    db.add(audit);db.flush()
    profile.contract_start=new_date;profile.employee.hire_date=new_date;db.flush()
    old_entry=db.query(PayrollBenefitAccrual).filter_by(employee_id=ident).order_by(PayrollBenefitAccrual.as_of.desc()).first()
    old_entry_id=old_entry.accounting_entry_id if old_entry else None
    if view['accounting']:
        accrued=b.sync_profile(db,profile,web.local_today(),user.email)
        if accrued.accounting_entry_id and accrued.accounting_entry_id!=old_entry_id:
            audit.adjustment_entry_id=accrued.accounting_entry_id
            entry=db.get(web.AccountingEntry,audit.adjustment_entry_id)
            entry.referencia=f'FECHA-ING-{audit.id}'
            entry.descripcion=f'Corrección de fecha de ingreso · {profile.employee.full_name} · {reason}'[:400]
    ids=[]
    for period in view['periods']:
        count,error=payroll._calculate_period_records(db,period,employee_id=ident,commit=False)
        if error:raise ValueError(error)
        calculation=db.query(PayrollCalculation).filter_by(period_id=period.id,employee_id=ident).one()
        if calculation.net_pay<0:raise ValueError('La planilla '+period.code+' quedaría con neto negativo. Revisa primero las deducciones o ajustes del borrador')
        ids.append(period.id)
    for row,values in view['draft_settlements']:
        for field in ('service_days','vacation_days','vacation_amount','bonus_days','bonus_amount','seniority_days','seniority_amount','salary_snapshot','gross_amount','net_amount'):setattr(row,field,values[field])
    audit.draft_period_ids=json.dumps(ids);audit.draft_settlement_ids=json.dumps([r.id for r,_ in view['draft_settlements']])
    db.flush()
    return audit
