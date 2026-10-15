"""Prestaciones: saldo laboral, provisión acumulada y salida se mantienen separados."""
from calendar import monthrange
from datetime import date, timedelta, datetime
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from sqlalchemy import func
from . import accounting_payroll as ledger
from ..models.payroll import (PayrollEmployeeProfile, PayrollBenefitSetting, PayrollBenefitAccrual,
    PayrollVacationMovement, PayrollVacationRelease, PayrollPeriod, PayrollSettlement, PayrollSettlementDeduction)
from ..models.sales import AccountingEntryLine

ZERO = Decimal('0')


def money(value):
    return Decimal(str(value or 0)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


def month_days(start, end):
    """2.5 por mes calendario completo; fracción proporcional a días reales del mes."""
    total = ZERO
    if not start or end < start:
        return total
    current = start
    while current <= end:
        last = date(current.year, current.month, monthrange(current.year, current.month)[1])
        stop = min(last, end)
        total += Decimal((stop-current).days+1) * Decimal('2.5') / Decimal(last.day)
        current = stop + timedelta(days=1)
    return total


def balance(db, profile, as_of, exclude_settlement=None):
    start = profile.contract_start or profile.employee.hire_date
    if profile.vacation_paid_through:
        start = max(start, profile.vacation_paid_through + timedelta(days=1)) if start else profile.vacation_paid_through + timedelta(days=1)
    earned = month_days(start, as_of)
    query = db.query(PayrollVacationMovement).filter_by(employee_id=profile.employee_id, status='ACTIVE').filter(PayrollVacationMovement.effective_date <= as_of)
    if profile.vacation_paid_through:
        query = query.filter(PayrollVacationMovement.effective_date > profile.vacation_paid_through)
    if exclude_settlement:
        query = query.filter((PayrollVacationMovement.settlement_id.is_(None)) | (PayrollVacationMovement.settlement_id != exclude_settlement))
    used = sum((Decimal(row.days) for row in query.all()), ZERO)
    available = earned-used
    if available < Decimal('-.0001'):
        raise ValueError('El historial de vacaciones supera los días acumulados. Revisa las fechas antes de continuar')
    available = max(ZERO, available)
    return {'earned': earned, 'used': used, 'days': available, 'amount': money(available * Decimal(profile.monthly_salary) / 30)}


def config(db):
    setting = db.get(PayrollBenefitSetting, 1)
    if not setting or not setting.enabled:
        raise ValueError('Activa primero la provisión de prestaciones y revisa sus cuentas y saldo inicial')
    expense = ledger.account(db, setting.expense_id)
    if expense.tipo != 'RESULTADO' or expense.naturaleza != 'DEBE':
        raise ValueError('El gasto de prestaciones debe ser una cuenta de resultado al Debe')
    liabilities = [ledger.account(db, getattr(setting, field)) for field in ('vacation_payable_id', 'bonus_payable_id', 'seniority_payable_id')]
    if len({a.id for a in liabilities}) != 3 or any(a.tipo != 'BALANCE' or a.naturaleza != 'HABER' for a in liabilities):
        raise ValueError('Selecciona tres cuentas distintas de pasivo al Haber para vacaciones, aguinaldo e indemnización')
    opening = ledger.account(db, setting.opening_debit_id)
    if opening.id in {a.id for a in liabilities}:
        raise ValueError('La contrapartida inicial no puede ser una cuenta de prestaciones por pagar')
    return setting, expense, opening, liabilities


def rest_dates(db, employee_id, start, end):
    rows = db.query(PayrollVacationMovement).filter_by(employee_id=employee_id, kind='REST', status='ACTIVE').filter(PayrollVacationMovement.date_from <= end, PayrollVacationMovement.date_to >= start).all()
    days = set()
    for row in rows:
        day = max(start, row.date_from)
        while day <= min(end, row.date_to):
            days.add(day);day += timedelta(days=1)
    return days


def targets(db, profile, as_of):
    from ..routers.payroll import _benefit_preview
    preview = _benefit_preview(profile, profile.employee, as_of)
    vacation = balance(db, profile, as_of)['amount']
    # El descanso reservado reduce días disponibles; su pasivo se aplica al cerrar la planilla.
    rows = db.query(PayrollVacationMovement).filter_by(employee_id=profile.employee_id, kind='REST', status='ACTIVE').filter(PayrollVacationMovement.effective_date <= as_of).all()
    for row in rows:
        released = db.query(func.sum(PayrollVacationRelease.amount)).filter_by(movement_id=row.id).scalar() or ZERO
        vacation += max(ZERO, Decimal(row.amount)-Decimal(released))
    return [money(vacation), preview['bonus_amount'], preview['seniority_amount']]


def sync_profile(db, profile, as_of, actor):
    ledger.lock(db)
    setting, expense, opening, liabilities = config(db)
    from ..routers import web
    if as_of>web.local_today():raise ValueError('No se provisionan prestaciones con fecha futura; espera a finalizar el período')
    if ledger.amount(profile.monthly_salary)<=0 or not (profile.contract_start or profile.employee.hire_date):
        raise ValueError('Completa salario positivo y fecha de ingreso del trabajador: '+profile.employee.full_name)
    if as_of < setting.start_date:
        raise ValueError('La fecha es anterior al inicio del control contable de prestaciones')
    if profile.currency != 'NIO' or not profile.employee.branch_id:
        raise ValueError('Las prestaciones requieren salario en C$ y sucursal asignada: '+profile.employee.full_name)
    previous = db.query(PayrollBenefitAccrual).filter_by(employee_id=profile.employee_id).order_by(PayrollBenefitAccrual.as_of.desc()).first()
    if previous and as_of < previous.as_of:
        raise ValueError('No se puede modificar una provisión anterior al último acumulado registrado')
    expected = targets(db, profile, as_of)
    fields = ('vacation_amount','bonus_amount','seniority_amount')
    old = [Decimal(getattr(previous, field)) for field in fields] if previous else [ZERO]*3
    lines = []
    debit_account = expense if previous else opening
    for label, liability, value, before in zip(('vacaciones','aguinaldo','indemnización estimada'), liabilities, expected, old):
        difference = money(value-before)
        if difference:
            lines += [AccountingEntryLine(cuenta=debit_account, debe=max(ZERO,difference), haber=max(ZERO,-difference), descripcion=f'{profile.employee.full_name} · {label}'),
                      AccountingEntryLine(cuenta=liability, debe=max(ZERO,-difference), haber=max(ZERO,difference), descripcion=f'{profile.employee.full_name} · {label}')]
    entry = ledger.posted_entry(db, profile.employee, as_of, f'PREST-{profile.employee_id}-{as_of.isoformat()}', 'Provisión de prestaciones · '+profile.employee.full_name, lines, actor) if lines else None
    row = previous if previous and previous.as_of == as_of else PayrollBenefitAccrual(employee_id=profile.employee_id, as_of=as_of)
    row.salary = profile.monthly_salary
    for field, value in zip(fields, expected):setattr(row,field,value)
    if entry:row.accounting_entry_id=entry.id
    db.add(row);db.flush()
    return row


def release_rest(db, period, actor):
    setting = db.get(PayrollBenefitSetting, 1)
    if not setting or not setting.enabled:return
    setting, expense, opening, liabilities = config(db)
    movements = db.query(PayrollVacationMovement).join(PayrollEmployeeProfile, PayrollEmployeeProfile.employee_id==PayrollVacationMovement.employee_id).filter(PayrollVacationMovement.kind=='REST', PayrollVacationMovement.status=='ACTIVE', PayrollVacationMovement.date_from<=period.date_to, PayrollVacationMovement.date_to>=period.date_from).all()
    for move in movements:
        if move.employee.branch_id != period.branch_id:continue
        if db.query(PayrollVacationRelease.id).filter_by(movement_id=move.id,period_id=period.id).first():continue
        profile = db.query(PayrollEmployeeProfile).filter_by(employee_id=move.employee_id).one()
        last=db.query(PayrollBenefitAccrual).filter_by(employee_id=move.employee_id).order_by(PayrollBenefitAccrual.as_of.desc()).first()
        effective=max(period.date_to,last.as_of if last else period.date_to)
        sync_profile(db, profile, effective, actor)
        overlap = Decimal((min(move.date_to, period.date_to)-max(move.date_from, period.date_from)).days+1)
        paid = money(Decimal(move.amount)*overlap/Decimal(move.days))
        prior = Decimal(db.query(func.sum(PayrollVacationRelease.days)).filter_by(movement_id=move.id).scalar() or ZERO)
        if prior+overlap > Decimal(move.days):raise ValueError('Las planillas se superponen sobre el mismo descanso')
        if prior+overlap==Decimal(move.days):paid=money(Decimal(move.amount)-Decimal(db.query(func.sum(PayrollVacationRelease.amount)).filter_by(movement_id=move.id).scalar() or ZERO))
        entry = ledger.posted_entry(db, period, effective, f'VAC-REST-{move.id}-{period.id}', 'Vacaciones remuneradas incluidas en planilla', [AccountingEntryLine(cuenta=liabilities[0],debe=paid,haber=0,descripcion=move.employee.full_name), AccountingEntryLine(cuenta=ledger.account(db,ledger.settings(db).salary_expense_id),debe=0,haber=paid,descripcion='Aplicación de provisión sin duplicar gasto salarial')], actor)
        db.add(PayrollVacationRelease(movement_id=move.id,period_id=period.id,days=overlap,amount=paid,accounting_entry_id=entry.id));db.flush()
        latest=db.query(PayrollBenefitAccrual).filter_by(employee_id=move.employee_id,as_of=effective).one()
        latest.vacation_amount=money(Decimal(latest.vacation_amount)-paid)
        db.flush()


def record(db, profile, kind, effective, days, start, end, reason, token, actor, method='EFECTIVO', journal_id=None, warehouse=None):
    from ..routers import web
    ledger.lock(db)
    existing=db.query(PayrollVacationMovement).filter_by(operation_key=token).first()
    if existing:
        if existing.employee_id!=profile.employee_id:raise ValueError('La operación pertenece a otro trabajador')
        if existing.status!='ACTIVE':raise ValueError('Esta operación ya fue anulada; inicia un registro nuevo')
        return existing
    if len(token)!=32 or kind not in {'REST','CASH'} or len(reason.strip())<5:raise ValueError('Completa la operación y su motivo')
    if effective!=web.local_today():raise ValueError('Registra el movimiento con la fecha actual; los saldos históricos se revisan por separado')
    if db.query(PayrollSettlement.id).filter_by(employee_id=profile.employee_id,status='DRAFT').first():raise ValueError('Anula o actualiza primero la liquidación pendiente del trabajador')
    if kind=='REST':
        if not start or not end or start<effective or end<start:raise ValueError('Selecciona un descanso desde hoy, con fecha final válida')
        days=Decimal((end-start).days+1)
        if rest_dates(db,profile.employee_id,start,end):raise ValueError('Ese descanso se superpone con vacaciones ya registradas')
        if db.query(PayrollPeriod.id).filter_by(branch_id=profile.employee.branch_id,status='CLOSED').filter(PayrollPeriod.date_from<=end,PayrollPeriod.date_to>=start).first():raise ValueError('El descanso cruza una planilla cerrada')
    days=Decimal(str(days))
    if not days.is_finite() or days<=0 or days!=days.quantize(Decimal('.0001')):raise ValueError('Escribe una cantidad positiva de días, con hasta cuatro decimales')
    available=balance(db,profile,effective)
    if days>available['days']:raise ValueError('El trabajador no tiene suficientes días de vacaciones disponibles')
    accrued=sync_profile(db,profile,effective,actor)
    setting, expense, opening, liabilities=config(db)
    value=money(days*Decimal(profile.monthly_salary)/30)
    move=PayrollVacationMovement(operation_key=token,employee_id=profile.employee_id,kind=kind,effective_date=effective,date_from=start if kind=='REST' else None,date_to=end if kind=='REST' else None,days=days,salary=profile.monthly_salary,amount=value,reason=reason.strip(),created_by=actor)
    db.add(move);db.flush()
    if kind=='CASH':
        if method not in {'EFECTIVO','TRANSFERENCIA','CHEQUE'}:raise ValueError('Forma de pago inválida')
        journal,source=ledger.payment_journal(db,profile.employee,method,journal_id)
        entry=ledger.posted_entry(db,profile.employee,effective,f'VAC-PAY-{move.id}','Pago de vacaciones · '+profile.employee.full_name,[AccountingEntryLine(cuenta=liabilities[0],debe=value,haber=0,descripcion=reason[:200]),AccountingEntryLine(cuenta=source,debe=0,haber=value,descripcion=profile.employee.full_name)],actor,voucher_code='EGRESO',journal=journal)
        move.accounting_entry_id=entry.id
        accrued.vacation_amount=money(Decimal(accrued.vacation_amount)-value)
        if method=='EFECTIVO':
            adapter=SimpleNamespace(branch_id=profile.employee.branch_id,amount=value,period=SimpleNamespace(accounting_payable_id=liabilities[0].id,code=f'Vacaciones {move.id} · {profile.employee.full_name}'))
            receipt=ledger.cash_movement(db,adapter,entry,warehouse,effective,'EGRESO',source.id,actor)
            move.cash_receipt_id=receipt.id
    db.flush();return move


def run_daily():
    from ..database import get_session_local
    from ..routers import web
    if not web._is_pacasholl_company():return
    with get_session_local()() as db:
        setting=db.get(PayrollBenefitSetting,1)
        if not setting or not setting.enabled:return
        try:
            ledger.lock(db)
            today=web.local_today()
            profiles=db.query(PayrollEmployeeProfile).filter_by(active=True).order_by(PayrollEmployeeProfile.employee_id).all()
            for profile in profiles:
                if profile.employee.status!='ACTIVE':continue
                previous=db.query(PayrollBenefitAccrual).filter_by(employee_id=profile.employee_id).order_by(PayrollBenefitAccrual.as_of.desc()).first()
                if previous and previous.as_of==today and money(previous.salary)!=money(profile.monthly_salary):
                    sync_profile(db,profile,today,'Sistema · ajuste de salario')
                cursor=max(setting.start_date, previous.as_of+timedelta(days=1) if previous else setting.start_date)
                while cursor<=today:
                    sync_profile(db,profile,cursor,'Sistema · provisión diaria')
                    cursor+=timedelta(days=1)
            setting.last_run=datetime.utcnow();setting.last_error=None;db.commit()
        except Exception as exc:
            db.rollback()
            setting=db.get(PayrollBenefitSetting,1)
            if setting:setting.last_error=str(getattr(exc,'detail',exc))[:1200];db.commit()
            import logging
            logging.getLogger(__name__).exception('No se pudo completar la provisión diaria de prestaciones')


def pay_settlement(db, row, profile, actor, method, journal_id, warehouse):
    from ..routers import web
    ledger.lock(db)
    if row.status!='DRAFT' or not profile or not profile.active:raise ValueError('La liquidación ya no está disponible para pago')
    today=web.local_today()
    if row.termination_date>today:raise ValueError('No se paga una liquidación con terminación futura')
    if db.query(PayrollVacationMovement.id).filter_by(employee_id=profile.employee_id,kind='REST',status='ACTIVE').filter(PayrollVacationMovement.date_to>row.termination_date).first():raise ValueError('Hay descansos que exceden la terminación; revisa el calendario antes de liquidar')
    # Una liquidación debe actualizarse si cambiaron sueldo, fecha o vacaciones.
    available=balance(db,profile,row.termination_date)
    if money(row.salary_snapshot)!=money(profile.monthly_salary) or abs(Decimal(row.vacation_days)-available['days'])>Decimal('.0001') or money(row.vacation_amount)!=available['amount']:
        raise ValueError('El saldo o salario cambió desde el borrador. Anula y vuelve a calcular la liquidación')
    if db.query(PayrollVacationMovement).filter_by(employee_id=profile.employee_id,kind='REST',status='ACTIVE').first():
        for move in db.query(PayrollVacationMovement).filter_by(employee_id=profile.employee_id,kind='REST',status='ACTIVE').all():
            released=Decimal(db.query(func.sum(PayrollVacationRelease.days)).filter_by(movement_id=move.id).scalar() or ZERO)
            if released!=Decimal(move.days):raise ValueError('Cierra primero las planillas que incluyen el descanso remunerado del trabajador')
    # No trasladar el cese hacia atrás cuando ya se provisionó después de él.
    last=db.query(PayrollBenefitAccrual).filter_by(employee_id=profile.employee_id).order_by(PayrollBenefitAccrual.as_of.desc()).first()
    effective=max(today,last.as_of if last else today)
    accrued=sync_profile(db,profile,effective,actor)
    setting,expense,opening,liabilities=config(db)
    journal,source=ledger.payment_journal(db,row,method,journal_id)
    if method not in {'EFECTIVO','TRANSFERENCIA','CHEQUE'}:raise ValueError('Forma de pago inválida')
    lines=[]
    expected=[money(row.vacation_amount),money(row.bonus_amount),money(row.seniority_amount)]
    old=[Decimal(accrued.vacation_amount),Decimal(accrued.bonus_amount),Decimal(accrued.seniority_amount)]
    # Ajustar la provisión hasta el importe realmente aprobado, incluida la elegibilidad de indemnización.
    for liability,actual,prior in zip(liabilities,expected,old):
        diff=money(actual-prior)
        if diff:lines.extend([AccountingEntryLine(cuenta=expense,debe=max(ZERO,diff),haber=max(ZERO,-diff),descripcion='Ajuste de prestación al liquidar'),AccountingEntryLine(cuenta=liability,debe=max(ZERO,-diff),haber=max(ZERO,diff),descripcion=row.employee.full_name)])
        if actual:lines.append(AccountingEntryLine(cuenta=liability,debe=actual,haber=0,descripcion='Liquidación · '+row.employee.full_name))
    if money(row.other_additions):lines.append(AccountingEntryLine(cuenta=expense,debe=money(row.other_additions),haber=0,descripcion='Adiciones aprobadas de liquidación'))
    allocated=ZERO
    for deduction_line in db.query(PayrollSettlementDeduction).filter_by(settlement_id=row.id).all():
        debt=deduction_line.employee_deduction
        from ..routers.payroll import _debt_paid
        already_paid,_=_debt_paid(db,debt.id)
        if money(deduction_line.amount)>money(debt.original_amount)-already_paid:raise ValueError('La deuda cambió desde el borrador; vuelve a calcular la liquidación')
        account=ledger.account(db,debt.deduction_type.accounting_account_id)
        value=money(deduction_line.amount);allocated+=value
        lines.append(AccountingEntryLine(cuenta=account,debe=0,haber=value,descripcion=debt.description[:200]))
    manual=money(row.deductions_amount)-allocated
    if manual<0 or money(row.deductions_amount)>money(row.gross_amount)-money(row.bonus_amount):raise ValueError('Revisa las deducciones; el aguinaldo no se consume con descuentos ordinarios')
    if manual:
        deduction=ledger.account(db,ledger.settings(db).manual_deduction_id)
        lines.append(AccountingEntryLine(cuenta=deduction,debe=0,haber=manual,descripcion='Deducción manual autorizada de liquidación'))
    if money(row.net_amount):lines.append(AccountingEntryLine(cuenta=source,debe=0,haber=money(row.net_amount),descripcion='Pago neto de liquidación'))
    entry=ledger.posted_entry(db,row,effective,f'LIQ-PAY-{row.id}','Liquidación final · '+row.employee.full_name,lines,actor,voucher_code='EGRESO',journal=journal)
    move=PayrollVacationMovement(operation_key=f'LIQ{row.id:029d}',employee_id=row.employee_id,kind='SETTLEMENT',effective_date=row.termination_date,days=row.vacation_days,salary=row.salary_snapshot,amount=row.vacation_amount,reason=row.reason_detail,settlement_id=row.id,accounting_entry_id=entry.id,created_by=actor)
    db.add(move);db.flush()
    if method=='EFECTIVO' and money(row.net_amount):
        adapter=SimpleNamespace(branch_id=row.branch_id,amount=row.net_amount,period=SimpleNamespace(accounting_payable_id=liabilities[0].id,code=f'Liquidación {row.id} · {row.employee.full_name}'))
        move.cash_receipt_id=ledger.cash_movement(db,adapter,entry,warehouse,effective,'EGRESO',source.id,actor).id
    accrued.vacation_amount=accrued.bonus_amount=accrued.seniority_amount=ZERO
    db.flush();return move


def void_movement(db, move, reason, actor):
    from ..routers import web
    from ..models.payroll import PayrollVacationReversal
    from ..models.sales import AccountingEntry, ReciboCaja, AccountingBankMatch, AccountingReconciliation
    from ..models.inventory import Bodega
    from sqlalchemy import or_
    ledger.lock(db)
    if move.status!='ACTIVE' or move.kind=='SETTLEMENT':raise ValueError('Solo se corrigen descansos o pagos activos desde esta sección')
    if len(reason.strip())<5:raise ValueError('Describe el motivo de la corrección')
    today=web.local_today()
    profile=db.query(PayrollEmployeeProfile).filter_by(employee_id=move.employee_id,active=True).first()
    if not profile:raise ValueError('El trabajador ya fue liquidado; revisa su liquidación antes de corregir vacaciones')
    if db.query(PayrollSettlement.id).filter_by(employee_id=move.employee_id,status='DRAFT').first():raise ValueError('Anula primero la liquidación en borrador')
    if move.kind=='REST' and (move.date_from<=today or db.query(PayrollVacationRelease.id).filter_by(movement_id=move.id).first()):raise ValueError('No se cancela un descanso que ya comenzó o fue aplicado en planilla')
    accrued=sync_profile(db,profile,today,actor)
    correction=PayrollVacationReversal(movement_id=move.id,reason=reason.strip(),created_by=actor)
    if move.kind=='CASH':
        original=db.get(AccountingEntry,move.accounting_entry_id)
        if not original or original.estado!='POSTEADO':raise ValueError('El pago no tiene un comprobante vigente para revertir')
        ids=[line.id for line in original.lines]
        if db.query(AccountingBankMatch.id).filter(AccountingBankMatch.canceled_at.is_(None),AccountingBankMatch.line_id.in_(ids)).first() or db.query(AccountingReconciliation.id).filter(AccountingReconciliation.canceled_at.is_(None),or_(AccountingReconciliation.debit_line_id.in_(ids),AccountingReconciliation.credit_line_id.in_(ids))).first():raise ValueError('Deshaz primero la conciliación de este pago')
        reversal=ledger.posted_entry(db,profile.employee,today,f'VAC-VOID-{move.id}','Anulación documentada de vacaciones · '+reason,[AccountingEntryLine(cuenta=line.cuenta,debe=line.haber,haber=line.debe,descripcion=line.descripcion) for line in original.lines],actor,reversal_of=original)
        original.estado='REVERSADO';correction.accounting_entry_id=reversal.id
        accrued.vacation_amount=money(Decimal(accrued.vacation_amount)+Decimal(move.amount))
        if move.cash_receipt_id:
            receipt=db.get(ReciboCaja,move.cash_receipt_id)
            adapter=SimpleNamespace(branch_id=profile.employee.branch_id,amount=move.amount,period=SimpleNamespace(accounting_payable_id=config(db)[3][0].id,code=f'Reintegro vacaciones {move.id} · {profile.employee.full_name}'))
            correction.cash_receipt_id=ledger.cash_movement(db,adapter,reversal,db.get(Bodega,receipt.bodega_id),today,'INGRESO',original.lines[-1].cuenta_id,actor).id
    move.status='VOID';db.add(correction);db.flush()
    sync_profile(db,profile,today,actor)
    return correction


def seed_accounts(db):
    """Completar solo subcuentas estándar ausentes; no modifica un catálogo personalizado."""
    from ..models.sales import CuentaContable
    parent=db.query(CuentaContable).filter_by(codigo='2113',tipo='BALANCE',naturaleza='HABER',activo=True).first()
    if not parent or (parent.nivel or 1)>=4:return
    for code,name in (('21131','Vacaciones por Pagar'),('21132','Aguinaldo por Pagar'),('21133','Provisión de Indemnización')):
        if not db.query(CuentaContable.id).filter_by(codigo=code).first():
            db.add(CuentaContable(codigo=code,nombre=name,tipo='BALANCE',naturaleza='HABER',parent_id=parent.id,nivel=parent.nivel+1,activo=True,descripcion='Control de prestaciones por trabajador; configuración revisable antes de su incorporación.'))
    db.flush()
