from calendar import monthrange
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from io import BytesIO
from typing import List, Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..core.deps import get_db
from ..models.attendance import AttendanceDeviceUser, AttendancePolicySetting, AttendancePunch, HRArea, HREmployee, HRPosition
from ..models.user import Branch
from ..models.payroll import (
    PayrollAdjustment,
    PayrollCalculation,
    PayrollCalculationDeduction,
    PayrollDeductionType,
    PayrollDeductionOverride,
    PayrollEmployeeDeduction,
    PayrollEmployeeDeductionSetting,
    PayrollEmployeeProfile,
    PayrollHoliday,
    PayrollPayment,
    PayrollPeriod,
    PayrollSettlement,
)
from .attendance import _browser_admin, _unique_catalog_code

router = APIRouter(tags=["Payroll"])
MONEY = Decimal("0.01")


def _money(value) -> Decimal:
    return Decimal(str(value or 0)).quantize(MONEY, rounding=ROUND_HALF_UP)


def _format_money(value) -> str:
    return f"{_money(value):,.2f}"


def _debt_paid(db: Session, debt_id: int) -> tuple[Decimal, int]:
    paid, installments = (
        db.query(func.sum(PayrollCalculationDeduction.amount), func.count(PayrollCalculationDeduction.id))
        .join(PayrollCalculation, PayrollCalculation.id == PayrollCalculationDeduction.calculation_id)
        .join(PayrollPeriod, PayrollPeriod.id == PayrollCalculation.period_id)
        .filter(
            PayrollCalculationDeduction.employee_deduction_id == debt_id,
            PayrollCalculationDeduction.amount > 0,
            PayrollPeriod.status == "CLOSED",
        )
        .one()
    )
    return _money(paid), int(installments or 0)


def _next_pay_date(value: date) -> date:
    if value.day <= 15:
        return value.replace(day=15)
    return value.replace(day=monthrange(value.year, value.month)[1])


def _advance_pay_date(value: date) -> date:
    if value.day <= 15:
        return value.replace(day=monthrange(value.year, value.month)[1])
    year = value.year + (1 if value.month == 12 else 0)
    month = 1 if value.month == 12 else value.month + 1
    return date(year, month, 15)


def _debt_schedule(db: Session, debt: PayrollEmployeeDeduction, visited=None) -> dict:
    visited = set(visited or ())
    paid, paid_installments = _debt_paid(db, debt.id)
    balance = max(Decimal("0"), _money(debt.original_amount) - paid)
    installment = max(MONEY, _money(debt.installment_amount))
    remaining_installments = int((balance / installment).to_integral_value(rounding=ROUND_CEILING)) if balance else 0
    dependency_waiting = False
    dependency_end = None
    dependency = None
    has_dependents = db.query(PayrollEmployeeDeduction.id).filter(
        PayrollEmployeeDeduction.depends_on_id == debt.id
    ).first() is not None
    if debt.depends_on_id and debt.depends_on_id not in visited:
        dependency = db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.id == debt.depends_on_id).first()
        if dependency:
            dependency_data = _debt_schedule(db, dependency, visited | {debt.id})
            dependency_waiting = dependency_data["balance"] > 0
            dependency_end = dependency_data["projected_end"]
    if balance <= 0:
        display_status = "PAGADA"
    elif debt.status == "PAUSED" and (not debt.paused_until or debt.paused_until > date.today()):
        display_status = "EN PAUSA"
    elif dependency_waiting:
        display_status = "ESPERANDO"
    else:
        display_status = "ACTIVA"
    projected_end = None
    if remaining_installments and not (debt.status == "PAUSED" and not debt.paused_until):
        projected_start = max(date.today(), debt.start_date, debt.paused_until or debt.start_date)
        if dependency_waiting:
            if dependency_end:
                projected_start = _advance_pay_date(dependency_end)
            else:
                projected_start = None
        if projected_start:
            projected_end = _next_pay_date(projected_start)
            for _ in range(remaining_installments - 1):
                projected_end = _advance_pay_date(projected_end)
    return {
        "paid": paid,
        "paid_installments": paid_installments,
        "balance": balance,
        "remaining_amount": balance,
        "remaining_installments": remaining_installments,
        "display_status": display_status,
        "projected_end": projected_end,
        "dependency_waiting": dependency_waiting,
        "dependency": dependency,
        "next_available_date": _advance_pay_date(projected_end) if projected_end else None,
        "has_dependents": has_dependents,
    }


def _next_employee_code(db: Session) -> str:
    next_id = (db.query(func.max(HREmployee.id)).scalar() or 0) + 1
    while True:
        code = f"EMP-{next_id:05d}"
        if not db.query(HREmployee.id).filter(HREmployee.employee_code == code).first():
            return code
        next_id += 1


def _benefit_preview(profile: PayrollEmployeeProfile, employee: HREmployee, as_of: date) -> dict:
    start = profile.contract_start or employee.hire_date
    salary = _money(profile.monthly_salary)
    if not start or as_of < start:
        return {"start": start, "service_days": 0, "vacation_days": Decimal("0"), "vacation_amount": Decimal("0"), "bonus_days": Decimal("0"), "bonus_amount": Decimal("0"), "seniority_days": Decimal("0"), "seniority_amount": Decimal("0")}
    service_days = (as_of - start).days + 1
    vacation_start = max(start, (profile.vacation_paid_through + timedelta(days=1)) if profile.vacation_paid_through else start)
    vacation_elapsed = max(0, (as_of - vacation_start).days + 1)
    vacation_days = (Decimal(vacation_elapsed) * Decimal("30") / Decimal("365"))
    statutory_bonus_start = date(as_of.year if as_of.month == 12 else as_of.year - 1, 12, 1)
    bonus_start = max(start, statutory_bonus_start, (profile.bonus_paid_through + timedelta(days=1)) if profile.bonus_paid_through else start)
    bonus_elapsed = max(0, (as_of - bonus_start).days + 1)
    bonus_days = Decimal(bonus_elapsed) * Decimal("30") / Decimal("365")
    seniority_start = max(start, (profile.seniority_paid_through + timedelta(days=1)) if profile.seniority_paid_through else start)
    seniority_elapsed = max(0, (as_of - seniority_start).days + 1)
    service_years = Decimal(seniority_elapsed) / Decimal("365")
    seniority_days = min(Decimal("150"), min(service_years, Decimal("3")) * Decimal("30") + max(Decimal("0"), service_years - Decimal("3")) * Decimal("20"))
    return {"start": start, "service_days": service_days, "vacation_days": vacation_days, "vacation_amount": _money(salary / 30 * vacation_days), "bonus_days": bonus_days, "bonus_amount": _money(salary / 30 * bonus_days), "seniority_days": seniority_days, "seniority_amount": _money(salary / 30 * seniority_days)}


@router.get("/payroll")
def payroll_home(request: Request, db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    employees = db.query(HREmployee).order_by(HREmployee.full_name).all()
    branches = db.query(Branch).filter(Branch.activo.is_(True)).order_by(Branch.name).all()
    profiles = {row.employee_id: row for row in db.query(PayrollEmployeeProfile).all()}
    deduction_types = db.query(PayrollDeductionType).filter(PayrollDeductionType.active.is_(True)).order_by(PayrollDeductionType.name).all()
    enabled_deductions = {
        (row.employee_id, row.deduction_type_id)
        for row in db.query(PayrollEmployeeDeductionSetting).filter(PayrollEmployeeDeductionSetting.enabled.is_(True)).all()
    }
    device_links = db.query(AttendanceDeviceUser).order_by(AttendanceDeviceUser.device_name).all()
    deductions = db.query(PayrollEmployeeDeduction).order_by(PayrollEmployeeDeduction.created_at.desc()).all()
    deduction_rows = []
    for item in deductions:
        schedule = _debt_schedule(db, item)
        deduction_rows.append({"item": item, **schedule})
    periods = db.query(PayrollPeriod).order_by(PayrollPeriod.date_from.desc()).all()
    selected_period_id = request.query_params.get("period_id")
    selected_period = None
    if selected_period_id and selected_period_id.isdigit():
        selected_period = db.query(PayrollPeriod).filter(PayrollPeriod.id == int(selected_period_id)).first()
    if not selected_period and periods:
        selected_period = periods[0]
    calculations = []
    eligible_profiles_count = 0
    employees_without_profile = 0
    if selected_period:
        calculations = (
            db.query(PayrollCalculation)
            .join(HREmployee, HREmployee.id == PayrollCalculation.employee_id)
            .filter(PayrollCalculation.period_id == selected_period.id)
            .filter(HREmployee.status == "ACTIVE")
            .order_by(PayrollCalculation.employee_id)
            .all()
        )
        eligible_profiles_count = (
            db.query(func.count(PayrollEmployeeProfile.id))
            .join(HREmployee)
            .filter(
                PayrollEmployeeProfile.active.is_(True),
                HREmployee.status == "ACTIVE",
                HREmployee.branch_id == selected_period.branch_id,
            )
            .scalar()
            or 0
        )
        employees_without_profile = (
            db.query(func.count(HREmployee.id))
            .outerjoin(PayrollEmployeeProfile, PayrollEmployeeProfile.employee_id == HREmployee.id)
            .filter(
                HREmployee.status == "ACTIVE",
                HREmployee.branch_id == selected_period.branch_id,
                PayrollEmployeeProfile.id.is_(None),
            )
            .scalar()
            or 0
        )
    deduction_overrides = {
        row.employee_deduction_id: row
        for row in db.query(PayrollDeductionOverride).filter(
            PayrollDeductionOverride.period_id == selected_period.id
        ).all()
    } if selected_period else {}
    period_deductions = {}
    if selected_period:
        for item in db.query(PayrollEmployeeDeduction).join(HREmployee).filter(
            HREmployee.branch_id == selected_period.branch_id,
            PayrollEmployeeDeduction.status.in_(["ACTIVE", "PAUSED"]),
            PayrollEmployeeDeduction.start_date <= selected_period.date_to,
        ).order_by(PayrollEmployeeDeduction.created_at).all():
            period_deductions.setdefault(item.employee_id, []).append(item)
    period_summary = None
    if selected_period:
        period_days = (selected_period.date_to - selected_period.date_from).days + 1
        cutoff = min(date.today(), selected_period.date_to)
        elapsed_days = max(0, min(period_days, (cutoff - selected_period.date_from).days + 1))
        period_summary = {
            "target_days": period_days,
            "elapsed_days": elapsed_days,
            "cutoff": cutoff,
            "base": sum((_money(row.base_pay) for row in calculations), Decimal("0")),
            "overtime_minutes": sum((row.overtime_minutes or 0 for row in calculations), 0),
            "overtime": sum((_money(row.overtime_pay) for row in calculations), Decimal("0")),
            "additions": sum((_money(row.additions_pay) + _money(row.holiday_pay) for row in calculations), Decimal("0")),
            "deductions": sum((_money(row.total_deductions) for row in calculations), Decimal("0")),
            "net": sum((_money(row.net_pay) for row in calculations), Decimal("0")),
            "projected_base": sum((_money(row.monthly_salary) / 2 for row in calculations), Decimal("0")),
        }
    return request.app.state.templates.TemplateResponse(
        "payroll.html",
        {
            "request": request,
            "user": user,
            "employees": employees,
            "next_employee_code": _next_employee_code(db),
            "branches": branches,
            "areas": db.query(HRArea).filter(HRArea.active.is_(True)).order_by(HRArea.name).all(),
            "positions": db.query(HRPosition).filter(HRPosition.active.is_(True)).order_by(HRPosition.name).all(),
            "profiles": profiles,
            "device_links": device_links,
            "deduction_types": deduction_types,
            "enabled_deductions": enabled_deductions,
            "deduction_rows": deduction_rows,
            "periods": periods,
            "selected_period": selected_period,
            "calculations": calculations,
            "period_summary": period_summary,
            "eligible_profiles_count": eligible_profiles_count,
            "employees_without_profile": employees_without_profile,
            "deduction_overrides": deduction_overrides,
            "period_deductions": period_deductions,
            "holidays": db.query(PayrollHoliday).order_by(PayrollHoliday.holiday_date.desc()).limit(50).all(),
            "payments": db.query(PayrollPayment).order_by(PayrollPayment.created_at.desc()).limit(30).all(),
        },
    )


@router.post("/payroll/employees")
def create_payroll_employee(
    request: Request,
    identification: str = Form(""),
    full_name: str = Form(...),
    branch_id: int = Form(...),
    area_id: str = Form(""),
    position_id: str = Form(""),
    monthly_salary: Decimal = Form(..., gt=0),
    contract_start: str = Form(""),
    device_user_link_id: str = Form(""),
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    if not db.query(Branch).filter(Branch.id == branch_id, Branch.activo.is_(True)).first():
        return RedirectResponse("/payroll?error=Sucursal+invalida", status_code=303)
    try:
        start_date = date.fromisoformat(contract_start) if contract_start.strip() else None
    except ValueError:
        return RedirectResponse("/payroll?error=Fecha+de+contrato+invalida", status_code=303)
    employee = HREmployee(
        employee_code=_next_employee_code(db),
        identification=identification.strip() or None,
        full_name=full_name.strip(),
        branch_id=branch_id,
        area_id=int(area_id) if area_id.isdigit() else None,
        position_id=int(position_id) if position_id.isdigit() else None,
        status="ACTIVE",
    )
    db.add(employee)
    try:
        db.flush()
        db.add(PayrollEmployeeProfile(employee_id=employee.id, monthly_salary=_money(monthly_salary), contract_start=start_date, pay_frequency="QUINCENAL"))
        if device_user_link_id.isdigit():
            link = db.query(AttendanceDeviceUser).filter(AttendanceDeviceUser.id == int(device_user_link_id)).first()
            if link:
                link.employee_id = employee.id
                db.query(AttendancePunch).filter(AttendancePunch.device_id == link.device_id, AttendancePunch.device_user_id == link.device_user_id).update({"employee_id": employee.id}, synchronize_session=False)
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse("/payroll?error=Codigo+o+cedula+del+empleado+duplicado", status_code=303)
    return RedirectResponse("/payroll?ok=Empleado+y+perfil+de+planilla+creados#profiles", status_code=303)


@router.post("/payroll/employees/{employee_id}/profile")
def save_employee_profile(
    employee_id: int,
    request: Request,
    monthly_salary: Decimal = Form(..., gt=0),
    contract_start: str = Form(""),
    device_user_link_id: str = Form(""),
    branch_id: str = Form(""),
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    employee = db.query(HREmployee).filter(HREmployee.id == employee_id).first()
    if not employee:
        raise HTTPException(404, "Empleado no encontrado")
    profile = db.query(PayrollEmployeeProfile).filter(PayrollEmployeeProfile.employee_id == employee_id).first()
    if not profile:
        profile = PayrollEmployeeProfile(employee_id=employee_id)
        db.add(profile)
    profile.monthly_salary = _money(monthly_salary)
    try:
        profile.contract_start = date.fromisoformat(contract_start) if contract_start.strip() else None
    except ValueError:
        return RedirectResponse("/payroll?error=Fecha+de+contrato+invalida", status_code=303)
    profile.pay_frequency = "QUINCENAL"
    if branch_id.strip().isdigit():
        employee.branch_id = int(branch_id)
    if device_user_link_id.strip().isdigit():
        link = db.query(AttendanceDeviceUser).filter(AttendanceDeviceUser.id == int(device_user_link_id)).first()
        if link:
            link.employee_id = employee_id
            db.query(AttendancePunch).filter(
                AttendancePunch.device_id == link.device_id,
                AttendancePunch.device_user_id == link.device_user_id,
            ).update({"employee_id": employee_id}, synchronize_session=False)
    db.commit()
    return RedirectResponse("/payroll?ok=Perfil+laboral+actualizado", status_code=303)


@router.post("/payroll/employees/{employee_id}/status")
def toggle_employee_status(employee_id: int, request: Request, db: Session = Depends(get_db)):
    _browser_admin(request, db)
    employee = db.query(HREmployee).filter(HREmployee.id == employee_id).first()
    if not employee:
        raise HTTPException(404, "Empleado no encontrado")

    activating = employee.status != "ACTIVE"
    employee.status = "ACTIVE" if activating else "INACTIVE"
    profile = db.query(PayrollEmployeeProfile).filter(
        PayrollEmployeeProfile.employee_id == employee.id
    ).first()
    if profile:
        profile.active = activating
    db.commit()
    action = "activado" if activating else "desactivado"
    message = quote_plus(f"Empleado {employee.full_name} {action} correctamente")
    return RedirectResponse(f"/payroll?ok={message}#profiles", status_code=303)


@router.post("/payroll/employees/{employee_id}/deduction-settings")
def save_employee_deduction_settings(
    employee_id: int,
    request: Request,
    enabled_type_ids: List[int] = Form(default=[]),
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    if not db.query(HREmployee).filter(HREmployee.id == employee_id).first():
        raise HTTPException(404, "Empleado no encontrado")
    valid_ids = {
        row.id for row in db.query(PayrollDeductionType).filter(PayrollDeductionType.active.is_(True)).all()
    }
    selected = set(enabled_type_ids) & valid_ids
    existing = {
        row.deduction_type_id: row
        for row in db.query(PayrollEmployeeDeductionSetting).filter(
            PayrollEmployeeDeductionSetting.employee_id == employee_id
        ).all()
    }
    for type_id in valid_ids:
        setting = existing.get(type_id)
        if not setting:
            setting = PayrollEmployeeDeductionSetting(employee_id=employee_id, deduction_type_id=type_id)
            db.add(setting)
        setting.enabled = type_id in selected
    db.commit()
    return RedirectResponse("/payroll?ok=Aplicacion+de+deducciones+actualizada#profiles", status_code=303)


@router.post("/payroll/employees/branch")
def assign_employee_branch(request: Request, employee_id: int = Form(...), branch_id: int = Form(...), db: Session = Depends(get_db)):
    _browser_admin(request, db)
    employee = db.query(HREmployee).filter(HREmployee.id == employee_id).first()
    branch = db.query(Branch).filter(Branch.id == branch_id, Branch.activo.is_(True)).first()
    if not employee or not branch:
        return RedirectResponse("/payroll?error=Empleado+o+sucursal+invalida", status_code=303)
    employee.branch_id = branch.id
    db.commit()
    return RedirectResponse("/payroll?ok=Sucursal+del+empleado+actualizada#profiles", status_code=303)


@router.post("/payroll/deduction-types")
def create_deduction_type(request: Request, name: str = Form(...), category: str = Form("OTHER"), is_loan: Optional[str] = Form(None), db: Session = Depends(get_db)):
    _browser_admin(request, db)
    db.add(PayrollDeductionType(code=_unique_catalog_code(db, PayrollDeductionType, name), name=name.strip(), category=category, is_loan=is_loan == "on"))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse("/payroll?error=Tipo+de+deduccion+duplicado", status_code=303)
    return RedirectResponse("/payroll?ok=Tipo+de+deduccion+creado", status_code=303)


@router.post("/payroll/deductions")
def create_employee_deduction(request: Request, employee_id: int = Form(...), deduction_type_id: int = Form(...), description: str = Form(...), original_amount: Decimal = Form(..., gt=0), installment_count: int = Form(..., ge=1, le=240), start_date: date = Form(...), depends_on_id: str = Form(""), notes: str = Form(""), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    dependency = None
    if depends_on_id.isdigit():
        dependency = db.query(PayrollEmployeeDeduction).filter(
            PayrollEmployeeDeduction.id == int(depends_on_id),
            PayrollEmployeeDeduction.employee_id == employee_id,
        ).first()
        if not dependency:
            return RedirectResponse("/payroll?error=La+deuda+dependiente+debe+pertenecer+al+mismo+empleado#deductions", status_code=303)
    amount = _money(original_amount)
    installment = (amount / installment_count).quantize(MONEY, rounding=ROUND_HALF_UP)
    if dependency:
        dependency_end = _debt_schedule(db, dependency)["projected_end"]
        if dependency_end:
            start_date = _advance_pay_date(dependency_end)
    debt = PayrollEmployeeDeduction(employee_id=employee_id, deduction_type_id=deduction_type_id, description=description.strip(), original_amount=amount, installment_count=installment_count, installment_amount=installment, start_date=start_date, depends_on_id=dependency.id if dependency else None, notes=notes.strip() or None, updated_by=user.email)
    db.add(debt)
    db.flush()
    setting = db.query(PayrollEmployeeDeductionSetting).filter(
        PayrollEmployeeDeductionSetting.employee_id == employee_id,
        PayrollEmployeeDeductionSetting.deduction_type_id == deduction_type_id,
    ).first()
    if not setting:
        setting = PayrollEmployeeDeductionSetting(employee_id=employee_id, deduction_type_id=deduction_type_id)
        db.add(setting)
    setting.enabled = True
    _refresh_debt_in_draft_calculations(db, debt)
    db.commit()
    return RedirectResponse("/payroll?ok=Deuda+registrada+y+planificada#deductions", status_code=303)


def _valid_debt_dependency(db: Session, debt: PayrollEmployeeDeduction, dependency_id: Optional[int]) -> bool:
    if not dependency_id:
        return True
    current_id = dependency_id
    visited = {debt.id}
    while current_id:
        if current_id in visited:
            return False
        visited.add(current_id)
        row = db.query(PayrollEmployeeDeduction).filter(
            PayrollEmployeeDeduction.id == current_id,
            PayrollEmployeeDeduction.employee_id == debt.employee_id,
        ).first()
        if not row:
            return False
        current_id = row.depends_on_id
    return True


def _refresh_debt_in_draft_calculations(db: Session, debt: PayrollEmployeeDeduction) -> None:
    paid, paid_installments = _debt_paid(db, debt.id)
    balance = max(Decimal("0"), _money(debt.original_amount) - paid)
    calculations = (
        db.query(PayrollCalculation)
        .join(PayrollPeriod, PayrollPeriod.id == PayrollCalculation.period_id)
        .filter(
            PayrollCalculation.employee_id == debt.employee_id,
            PayrollPeriod.status == "DRAFT",
        )
        .all()
    )
    dependency_ready = True
    if debt.depends_on_id:
        dependency = db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.id == debt.depends_on_id).first()
        if dependency:
            dependency_paid, _ = _debt_paid(db, dependency.id)
            dependency_ready = _money(dependency.original_amount) - dependency_paid <= 0
    for calculation in calculations:
        line = next((row for row in calculation.deduction_lines if row.employee_deduction_id == debt.id), None)
        period = calculation.period
        paused = debt.status == "PAUSED" and (not debt.paused_until or debt.paused_until > period.date_to)
        should_charge = balance > 0 and debt.status != "COMPLETED" and debt.start_date <= period.date_to and not paused and dependency_ready
        amount = min(_money(debt.installment_amount), balance) if should_charge else Decimal("0")
        if line:
            line.amount = amount
            line.installment_number = paid_installments + 1
        elif should_charge:
            calculation.deduction_lines.append(PayrollCalculationDeduction(employee_deduction_id=debt.id, amount=amount, installment_number=paid_installments + 1))
        _update_calculation_totals(db, calculation)


def _refresh_debt_tree(db: Session, debt: PayrollEmployeeDeduction, visited: Optional[set[int]] = None) -> None:
    """Recalculate a debt and every debt whose schedule depends on it."""
    visited = visited or set()
    if debt.id in visited:
        return
    visited.add(debt.id)
    _refresh_debt_in_draft_calculations(db, debt)
    children = db.query(PayrollEmployeeDeduction).filter(
        PayrollEmployeeDeduction.depends_on_id == debt.id
    ).all()
    for child in children:
        _refresh_debt_tree(db, child, visited)


@router.post("/payroll/deductions/{deduction_id}/edit")
def edit_employee_deduction(
    deduction_id: int,
    request: Request,
    deduction_type_id: int = Form(...),
    description: str = Form(...),
    remaining_amount: Decimal = Form(..., ge=0),
    future_installments: int = Form(..., ge=1, le=240),
    start_date: date = Form(...),
    depends_on_id: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db),
):
    user = _browser_admin(request, db)
    debt = db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.id == deduction_id).first()
    if not debt:
        raise HTTPException(404, "Deuda no encontrada")
    deduction_type = db.query(PayrollDeductionType).filter(PayrollDeductionType.id == deduction_type_id).first()
    if not deduction_type:
        return RedirectResponse("/payroll?error=Tipo+de+deduccion+invalido#deductions", status_code=303)
    dependency_id = int(depends_on_id) if depends_on_id.isdigit() else None
    if not _valid_debt_dependency(db, debt, dependency_id):
        return RedirectResponse("/payroll?error=Dependencia+invalida+o+circular#deductions", status_code=303)
    dependency = db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.id == dependency_id).first() if dependency_id else None
    if dependency:
        dependency_end = _debt_schedule(db, dependency)["projected_end"]
        if dependency_end:
            start_date = _advance_pay_date(dependency_end)
    paid, paid_installments = _debt_paid(db, debt.id)
    balance = _money(remaining_amount)
    debt.deduction_type_id = deduction_type.id
    setting = db.query(PayrollEmployeeDeductionSetting).filter(
        PayrollEmployeeDeductionSetting.employee_id == debt.employee_id,
        PayrollEmployeeDeductionSetting.deduction_type_id == deduction_type.id,
    ).first()
    if not setting:
        setting = PayrollEmployeeDeductionSetting(
            employee_id=debt.employee_id,
            deduction_type_id=deduction_type.id,
        )
        db.add(setting)
    setting.enabled = True
    debt.description = description.strip()
    debt.original_amount = _money(paid + balance)
    debt.installment_count = paid_installments + future_installments
    debt.installment_amount = _money(balance / future_installments) if balance else MONEY
    debt.start_date = start_date
    debt.depends_on_id = dependency_id
    debt.notes = notes.strip() or None
    debt.updated_by = user.email
    debt.updated_at = datetime.utcnow()
    if balance <= 0:
        debt.status = "COMPLETED"
    elif debt.status == "COMPLETED":
        debt.status = "ACTIVE"
    _refresh_debt_tree(db, debt)
    db.commit()
    return RedirectResponse("/payroll?ok=Deuda+actualizada+conservando+los+pagos+aplicados#deductions", status_code=303)


@router.post("/payroll/deductions/{deduction_id}/status")
def update_employee_deduction_status(
    deduction_id: int,
    request: Request,
    action: str = Form(...),
    pause_reason: str = Form(""),
    paused_until: str = Form(""),
    db: Session = Depends(get_db),
):
    user = _browser_admin(request, db)
    debt = db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.id == deduction_id).first()
    if not debt:
        raise HTTPException(404, "Deuda no encontrada")
    if action == "pause":
        if len(pause_reason.strip()) < 2:
            return RedirectResponse("/payroll?error=Indique+el+motivo+de+la+pausa#deductions", status_code=303)
        try:
            resume_date = date.fromisoformat(paused_until) if paused_until.strip() else None
        except ValueError:
            return RedirectResponse("/payroll?error=Fecha+de+reanudacion+invalida#deductions", status_code=303)
        debt.status = "PAUSED"
        debt.paused_until = resume_date
        debt.pause_reason = pause_reason.strip()
        debt.paused_at = datetime.utcnow()
    elif action == "resume":
        debt.status = "ACTIVE"
        debt.paused_until = None
        debt.pause_reason = None
        debt.paused_at = None
    else:
        return RedirectResponse("/payroll?error=Accion+de+deuda+invalida#deductions", status_code=303)
    debt.updated_by = user.email
    debt.updated_at = datetime.utcnow()
    _refresh_debt_tree(db, debt)
    db.commit()
    return RedirectResponse("/payroll?ok=Calendario+de+cobro+actualizado#deductions", status_code=303)


@router.post("/payroll/deductions/{deduction_id}/delete")
def delete_employee_deduction(
    deduction_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    debt = db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.id == deduction_id).first()
    if not debt:
        return RedirectResponse("/payroll?error=Deuda+no+encontrada#deductions", status_code=303)
    paid_amount, paid_installments = _debt_paid(db, debt.id)
    if paid_amount > 0 or paid_installments > 0:
        return RedirectResponse("/payroll?error=No+se+puede+eliminar+una+deuda+con+pagos+aplicados#deductions", status_code=303)
    if db.query(PayrollEmployeeDeduction.id).filter(PayrollEmployeeDeduction.depends_on_id == debt.id).first():
        return RedirectResponse("/payroll?error=Quite+primero+las+deudas+que+dependen+de+este+registro#deductions", status_code=303)

    calculation_ids = [
        row[0]
        for row in db.query(PayrollCalculationDeduction.calculation_id).filter(
            PayrollCalculationDeduction.employee_deduction_id == debt.id
        ).distinct().all()
    ]
    db.query(PayrollDeductionOverride).filter(
        PayrollDeductionOverride.employee_deduction_id == debt.id
    ).delete(synchronize_session=False)
    db.query(PayrollCalculationDeduction).filter(
        PayrollCalculationDeduction.employee_deduction_id == debt.id
    ).delete(synchronize_session=False)
    db.delete(debt)
    db.flush()
    for calculation in db.query(PayrollCalculation).filter(PayrollCalculation.id.in_(calculation_ids)).all() if calculation_ids else []:
        _update_calculation_totals(db, calculation)
    db.commit()
    return RedirectResponse("/payroll?ok=Deuda+eliminada+sin+afectar+pagos#deductions", status_code=303)


@router.post("/payroll/periods")
def create_period(request: Request, branch_id: int = Form(...), date_from: date = Form(...), date_to: date = Form(...), pay_date: date = Form(...), db: Session = Depends(get_db)):
    _browser_admin(request, db)
    if date_to < date_from or (date_to - date_from).days + 1 > 16:
        return RedirectResponse("/payroll?error=El+periodo+debe+tener+entre+1+y+16+dias", status_code=303)
    branch = db.query(Branch).filter(Branch.id == branch_id, Branch.activo.is_(True)).first()
    if not branch:
        return RedirectResponse("/payroll?error=Sucursal+invalida", status_code=303)
    code = f"{branch.code}-{date_from:%Y%m%d}-{date_to:%Y%m%d}"
    db.add(PayrollPeriod(code=code, branch_id=branch.id, date_from=date_from, date_to=date_to, pay_date=pay_date))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse("/payroll?error=Periodo+duplicado", status_code=303)
    return RedirectResponse("/payroll?ok=Periodo+quincenal+creado", status_code=303)


@router.post("/payroll/holidays")
def create_holiday(request: Request, holiday_date: date = Form(...), name: str = Form(...), period_id: str = Form(""), db: Session = Depends(get_db)):
    _browser_admin(request, db)
    db.add(PayrollHoliday(holiday_date=holiday_date, name=name.strip(), period_id=int(period_id) if period_id.isdigit() else None, paid=True, worked_as_overtime=True))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse("/payroll?error=Feriado+duplicado", status_code=303)
    return RedirectResponse("/payroll?ok=Feriado+registrado", status_code=303)


def _employee_time(db: Session, employee_id: int, period: PayrollPeriod, policy: AttendancePolicySetting, holidays: dict):
    start = datetime.combine(period.date_from, time.min)
    end = datetime.combine(period.date_to + timedelta(days=1), time.min)
    punches = db.query(AttendancePunch).filter(AttendancePunch.employee_id == employee_id, AttendancePunch.occurred_at >= start, AttendancePunch.occurred_at < end).order_by(AttendancePunch.occurred_at).all()
    by_date = {}
    for punch in punches:
        by_date.setdefault(punch.occurred_at.date(), []).append(punch.occurred_at)
    overtime_minutes = 0
    holiday_minutes = 0
    for day, marks in by_date.items():
        if len(marks) < 2:
            continue
        entry, exit_at = marks[0], marks[-1]
        gross = max(0, int((exit_at - entry).total_seconds() // 60))
        worked = max(0, gross - (policy.break_minutes if gross >= policy.break_after_minutes else 0))
        if day in holidays and holidays[day].worked_as_overtime:
            holiday_minutes += worked
        elif day.weekday() == 6 and policy.sunday_all_day_overtime:
            overtime_minutes += worked
        elif day.weekday() == 5:
            cutoff = datetime.combine(day, policy.saturday_overtime_start)
            overtime_minutes += max(0, int((exit_at - max(entry, cutoff)).total_seconds() // 60))
        else:
            cutoff = datetime.combine(day, policy.weekday_overtime_start)
            overtime_minutes += max(0, int((exit_at - max(entry, cutoff)).total_seconds() // 60))
    return len(by_date), overtime_minutes, holiday_minutes


@router.post("/payroll/periods/{period_id}/calculate")
def calculate_period(period_id: int, request: Request, db: Session = Depends(get_db)):
    _browser_admin(request, db)
    period = db.query(PayrollPeriod).filter(PayrollPeriod.id == period_id).first()
    if not period or period.status == "CLOSED":
        return RedirectResponse("/payroll?error=Periodo+no+disponible", status_code=303)
    policy = db.query(AttendancePolicySetting).first()
    if not policy:
        return RedirectResponse("/payroll?error=Configure+primero+la+politica+de+marcadas", status_code=303)
    holidays = {row.holiday_date: row for row in db.query(PayrollHoliday).filter(PayrollHoliday.holiday_date >= period.date_from, PayrollHoliday.holiday_date <= period.date_to).all()}
    profiles_query = db.query(PayrollEmployeeProfile).join(HREmployee).filter(
        PayrollEmployeeProfile.active.is_(True),
        HREmployee.status == "ACTIVE",
    )
    if period.branch_id:
        profiles_query = profiles_query.filter(HREmployee.branch_id == period.branch_id)
    profiles = profiles_query.all()
    if not profiles:
        branch_name = period.branch.name if period.branch else "seleccionada"
        message = quote_plus(f"No hay empleados activos con salario y perfil asignados a la sucursal {branch_name}")
        return RedirectResponse(f"/payroll?period_id={period.id}&error={message}", status_code=303)
    for profile in profiles:
        salary = _money(profile.monthly_salary)
        hourly = salary / Decimal("240")
        _attendance_days, overtime_minutes, holiday_minutes = _employee_time(db, profile.employee_id, period, policy, holidays)
        overtime_pay = _money((Decimal(overtime_minutes) / 60) * hourly * 2)
        holiday_pay = _money((Decimal(holiday_minutes) / 60) * hourly * 2)
        calc = db.query(PayrollCalculation).filter(PayrollCalculation.period_id == period.id, PayrollCalculation.employee_id == profile.employee_id).first()
        if not calc:
            calc = PayrollCalculation(period_id=period.id, employee_id=profile.employee_id, monthly_salary=salary, base_pay=0, gross_pay=0, net_pay=0)
            db.add(calc)
            db.flush()
        else:
            calc.deduction_lines.clear()
            db.flush()
        total_deductions = Decimal("0")
        enabled_type_ids = {
            row.deduction_type_id
            for row in db.query(PayrollEmployeeDeductionSetting).filter(
                PayrollEmployeeDeductionSetting.employee_id == profile.employee_id,
                PayrollEmployeeDeductionSetting.enabled.is_(True),
            ).all()
        }
        active_deductions = db.query(PayrollEmployeeDeduction).filter(
            PayrollEmployeeDeduction.employee_id == profile.employee_id,
            PayrollEmployeeDeduction.deduction_type_id.in_(enabled_type_ids),
            PayrollEmployeeDeduction.status.in_(["ACTIVE", "PAUSED"]),
            PayrollEmployeeDeduction.start_date <= period.date_to,
        ).all() if enabled_type_ids else []
        overrides = {
            row.employee_deduction_id: row
            for row in db.query(PayrollDeductionOverride).filter(
                PayrollDeductionOverride.period_id == period.id,
                PayrollDeductionOverride.employee_deduction_id.in_([row.id for row in active_deductions]),
            ).all()
        } if active_deductions else {}
        for deduction in active_deductions:
            paid_amount, applied_count = _debt_paid(db, deduction.id)
            remaining = max(Decimal("0"), _money(deduction.original_amount) - paid_amount)
            if remaining <= 0:
                deduction.status = "COMPLETED"
                continue
            if deduction.status == "PAUSED":
                if not deduction.paused_until or deduction.paused_until > period.date_to:
                    continue
                deduction.status = "ACTIVE"
                deduction.paused_until = None
                deduction.pause_reason = None
                deduction.paused_at = None
            if deduction.depends_on_id:
                dependency = db.query(PayrollEmployeeDeduction).filter(
                    PayrollEmployeeDeduction.id == deduction.depends_on_id
                ).first()
                if dependency:
                    dependency_paid, _ = _debt_paid(db, dependency.id)
                    if max(Decimal("0"), _money(dependency.original_amount) - dependency_paid) > 0:
                        continue
            override = overrides.get(deduction.id)
            if override and not override.apply_charge:
                continue
            amount = min(_money(deduction.installment_amount), remaining)
            if override and override.override_amount is not None:
                amount = min(_money(override.override_amount), remaining)
            calc.deduction_lines.append(PayrollCalculationDeduction(employee_deduction_id=deduction.id, amount=amount, installment_number=applied_count + 1))
            total_deductions += amount
        adjustments = db.query(PayrollAdjustment).filter(PayrollAdjustment.period_id == period.id, PayrollAdjustment.employee_id == profile.employee_id, PayrollAdjustment.active.is_(True)).order_by(PayrollAdjustment.created_at, PayrollAdjustment.id).all()
        additions = _money(sum((row.amount for row in adjustments if row.adjustment_type == "ADDITION"), Decimal("0")))
        manual_deductions = _money(sum((row.amount for row in adjustments if row.adjustment_type == "DEDUCTION"), Decimal("0")))
        manual_days = next((row.worked_days for row in reversed(adjustments) if row.adjustment_type == "WORKED_DAYS" and row.worked_days is not None), None)
        manual_overtime = next((row.worked_days for row in reversed(adjustments) if row.adjustment_type == "OVERTIME_MINUTES" and row.worked_days is not None), None)
        if manual_overtime is not None:
            overtime_minutes = max(0, manual_overtime)
            overtime_pay = _money((Decimal(overtime_minutes) / 60) * hourly * 2)
        target_days = (period.date_to - period.date_from).days + 1
        cutoff = min(date.today(), period.date_to)
        employment_start = period.date_from
        employee_start = profile.contract_start or profile.employee.hire_date
        if employee_start:
            employment_start = max(employment_start, employee_start)
        accrued_calendar_days = max(0, (cutoff - employment_start).days + 1)
        payable_days = max(0, min(target_days, manual_days if manual_days is not None else accrued_calendar_days))
        base = _money((salary / Decimal("30")) * Decimal(payable_days))
        gross = _money(base + overtime_pay + holiday_pay + additions)
        calc.monthly_salary, calc.base_pay, calc.days_worked = salary, base, payable_days
        calc.overtime_minutes, calc.overtime_pay, calc.holiday_pay = overtime_minutes, overtime_pay, holiday_pay
        calc.additions_pay = additions
        total_deductions += manual_deductions
        calc.gross_pay, calc.total_deductions, calc.net_pay = gross, _money(total_deductions), _money(gross - total_deductions)
        calc.calculated_at = datetime.utcnow()
    db.commit()
    message = quote_plus(f"Planilla acumulada al corte actual: {len(profiles)} empleado(s) recalculado(s)")
    return RedirectResponse(f"/payroll?period_id={period.id}&ok={message}", status_code=303)


def _update_calculation_totals(db: Session, calculation: PayrollCalculation) -> None:
    planned = sum((_money(line.amount) for line in calculation.deduction_lines), Decimal("0"))
    adjustments = db.query(PayrollAdjustment).filter(
        PayrollAdjustment.period_id == calculation.period_id,
        PayrollAdjustment.employee_id == calculation.employee_id,
        PayrollAdjustment.active.is_(True),
    ).all()
    manual_deductions = sum((_money(row.amount) for row in adjustments if row.adjustment_type == "DEDUCTION"), Decimal("0"))
    additions = sum((_money(row.amount) for row in adjustments if row.adjustment_type == "ADDITION"), Decimal("0"))
    calculation.additions_pay = _money(additions)
    calculation.total_deductions = _money(planned + manual_deductions)
    calculation.gross_pay = _money(_money(calculation.base_pay) + _money(calculation.overtime_pay) + _money(calculation.holiday_pay) + additions)
    calculation.net_pay = _money(calculation.gross_pay - calculation.total_deductions)
    calculation.calculated_at = datetime.utcnow()


@router.post("/payroll/calculations/{calculation_id}/deductions/{deduction_id}")
def update_period_deduction(calculation_id: int, deduction_id: int, request: Request, action: str = Form(...), amount: str = Form(""), reason: str = Form(..., min_length=2), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    calculation = db.query(PayrollCalculation).filter(PayrollCalculation.id == calculation_id).first()
    deduction = db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.id == deduction_id).first()
    if not calculation or calculation.status == "CLOSED" or not deduction or deduction.employee_id != calculation.employee_id:
        return RedirectResponse("/payroll?error=Cambio+de+deduccion+no+permitido", status_code=303)
    paid_amount, paid_installments = _debt_paid(db, deduction.id)
    remaining = max(Decimal("0"), _money(deduction.original_amount) - paid_amount)
    row = db.query(PayrollDeductionOverride).filter(PayrollDeductionOverride.period_id == calculation.period_id, PayrollDeductionOverride.employee_deduction_id == deduction.id).first()
    if not row:
        row = PayrollDeductionOverride(period_id=calculation.period_id, employee_deduction_id=deduction.id, reason=reason.strip(), created_by=user.email)
        db.add(row)
    row.reason = reason.strip()
    row.created_by = user.email
    if action == "pause":
        row.apply_charge, row.override_amount = False, None
    elif action == "restore":
        row.apply_charge, row.override_amount = True, None
    elif action == "amount":
        try:
            temporary = _money(Decimal(amount))
        except Exception:
            return RedirectResponse(f"/payroll?period_id={calculation.period_id}&error=Monto+temporal+invalido", status_code=303)
        if temporary < 0 or temporary > remaining:
            return RedirectResponse(f"/payroll?period_id={calculation.period_id}&error=Monto+temporal+invalido", status_code=303)
        row.apply_charge, row.override_amount = True, temporary
    else:
        return RedirectResponse(f"/payroll?period_id={calculation.period_id}&error=Accion+invalida", status_code=303)
    line = next((item for item in calculation.deduction_lines if item.employee_deduction_id == deduction.id), None)
    if action == "pause" and line:
        line.amount = 0
    elif action == "restore":
        restored_amount = min(_money(deduction.installment_amount), remaining)
        if line:
            line.amount = restored_amount
        else:
            calculation.deduction_lines.append(PayrollCalculationDeduction(employee_deduction_id=deduction.id, amount=restored_amount, installment_number=paid_installments + 1))
    elif action == "amount":
        if line:
            line.amount = temporary
        else:
            calculation.deduction_lines.append(PayrollCalculationDeduction(employee_deduction_id=deduction.id, amount=temporary, installment_number=paid_installments + 1))
    db.flush()
    _update_calculation_totals(db, calculation)
    db.commit()
    return RedirectResponse(f"/payroll?period_id={calculation.period_id}&ok=Deduccion+actualizada+sin+alterar+la+deuda", status_code=303)


@router.post("/payroll/periods/{period_id}/close")
def close_period(period_id: int, request: Request, db: Session = Depends(get_db)):
    _browser_admin(request, db)
    period = db.query(PayrollPeriod).filter(PayrollPeriod.id == period_id).first()
    if not period or not db.query(PayrollCalculation).filter(PayrollCalculation.period_id == period_id).first():
        return RedirectResponse("/payroll?error=Calcule+la+planilla+antes+de+cerrar", status_code=303)
    period.status = "CLOSED"
    period.closed_at = datetime.utcnow()
    db.query(PayrollCalculation).filter(PayrollCalculation.period_id == period_id).update({"status": "CLOSED"}, synchronize_session=False)
    db.commit()
    return RedirectResponse(f"/payroll?period_id={period.id}&ok=Planilla+cerrada", status_code=303)


@router.post("/payroll/adjustments")
def create_adjustment(request: Request, period_id: int = Form(...), employee_id: int = Form(...), adjustment_type: str = Form(...), description: str = Form(...), amount: Decimal = Form(0), worked_days: str = Form(""), overtime_hours: str = Form(""), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    period = db.query(PayrollPeriod).filter(PayrollPeriod.id == period_id).first()
    employee = db.query(HREmployee).filter(HREmployee.id == employee_id).first()
    if not period or period.status == "CLOSED" or not employee or (period.branch_id and employee.branch_id != period.branch_id):
        return RedirectResponse("/payroll?error=Ajuste+no+permitido", status_code=303)
    kind = adjustment_type.upper()
    if kind not in {"ADDITION", "DEDUCTION", "WORKED_DAYS", "OVERTIME_MINUTES"}:
        return RedirectResponse("/payroll?error=Tipo+de+ajuste+invalido", status_code=303)
    days = int(worked_days) if worked_days.isdigit() else None
    if kind == "WORKED_DAYS" and (days is None or days < 0 or days > 16):
        return RedirectResponse("/payroll?error=Dias+trabajados+invalidos", status_code=303)
    if kind == "OVERTIME_MINUTES":
        try:
            overtime_minutes = int((Decimal(overtime_hours) * 60).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        except Exception:
            return RedirectResponse(f"/payroll?period_id={period.id}&error=Horas+extra+invalidas", status_code=303)
        if overtime_minutes < 0 or overtime_minutes > 240 * 60:
            return RedirectResponse(f"/payroll?period_id={period.id}&error=Horas+extra+invalidas", status_code=303)
        days = overtime_minutes
    if kind not in {"WORKED_DAYS", "OVERTIME_MINUTES"} and _money(amount) <= 0:
        return RedirectResponse("/payroll?error=Monto+de+ajuste+invalido", status_code=303)
    adjustment = PayrollAdjustment(period_id=period.id, employee_id=employee.id, adjustment_type=kind, description=description.strip(), amount=_money(amount), worked_days=days, created_by=user.email)
    db.add(adjustment)
    db.flush()
    calculation = db.query(PayrollCalculation).filter(PayrollCalculation.period_id == period.id, PayrollCalculation.employee_id == employee.id).first()
    if calculation and kind == "WORKED_DAYS":
        calculation.days_worked = days
        calculation.base_pay = _money((_money(calculation.monthly_salary) / Decimal("30")) * Decimal(days))
        _update_calculation_totals(db, calculation)
    elif calculation and kind == "OVERTIME_MINUTES":
        calculation.overtime_minutes = days
        calculation.overtime_pay = _money((Decimal(days) / Decimal("60")) * (_money(calculation.monthly_salary) / Decimal("240")) * 2)
        _update_calculation_totals(db, calculation)
    elif calculation and kind in {"ADDITION", "DEDUCTION"}:
        _update_calculation_totals(db, calculation)
    db.commit()
    return RedirectResponse(f"/payroll?period_id={period.id}&ok=Ajuste+registrado+y+neto+actualizado", status_code=303)


@router.post("/payroll/adjustments/{adjustment_id}/void")
def void_adjustment(adjustment_id: int, request: Request, reason: str = Form(...), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    row = db.query(PayrollAdjustment).filter(PayrollAdjustment.id == adjustment_id, PayrollAdjustment.active.is_(True)).first()
    if not row or row.period.status == "CLOSED":
        return RedirectResponse("/payroll/reports?error=Ajuste+no+anulable", status_code=303)
    row.active, row.void_reason, row.voided_by, row.voided_at = False, reason.strip(), user.email, datetime.utcnow()
    db.commit()
    return RedirectResponse(f"/payroll/reports?period_id={row.period_id}&ok=Ajuste+anulado", status_code=303)


@router.post("/payroll/payments")
def create_payment(request: Request, period_id: int = Form(...), payment_date: date = Form(...), payment_method: str = Form("EFECTIVO"), reference: str = Form(""), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    period = db.query(PayrollPeriod).filter(PayrollPeriod.id == period_id, PayrollPeriod.status == "CLOSED").first()
    if not period or not period.branch_id:
        return RedirectResponse("/payroll?error=Solo+puede+pagar+una+planilla+cerrada+con+sucursal", status_code=303)
    already_paid = db.query(PayrollPayment).filter(PayrollPayment.period_id == period.id, PayrollPayment.status == "PAID").first()
    if already_paid:
        return RedirectResponse("/payroll?error=La+planilla+ya+tiene+un+pago+activo", status_code=303)
    total = _money(db.query(func.sum(PayrollCalculation.net_pay)).filter(PayrollCalculation.period_id == period.id).scalar())
    db.add(PayrollPayment(period_id=period.id, branch_id=period.branch_id, amount=total, payment_date=payment_date, payment_method=payment_method, reference=reference.strip() or None, created_by=user.email))
    db.commit()
    return RedirectResponse(f"/payroll?period_id={period.id}&ok=Pago+de+planilla+registrado", status_code=303)


@router.post("/payroll/payments/{payment_id}/void")
def void_payment(payment_id: int, request: Request, reason: str = Form(...), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    payment = db.query(PayrollPayment).filter(PayrollPayment.id == payment_id, PayrollPayment.status == "PAID").first()
    if not payment:
        return RedirectResponse("/payroll?error=Pago+no+encontrado", status_code=303)
    payment.status, payment.void_reason, payment.voided_by, payment.voided_at = "VOID", reason.strip(), user.email, datetime.utcnow()
    db.commit()
    return RedirectResponse(f"/payroll?period_id={payment.period_id}&ok=Pago+anulado+con+trazabilidad", status_code=303)


@router.get("/payroll/reports")
def payroll_reports(request: Request, period_id: str = "", branch_id: str = "", db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    branches = db.query(Branch).filter(Branch.activo.is_(True)).order_by(Branch.name).all()
    periods_query = db.query(PayrollPeriod).order_by(PayrollPeriod.date_from.desc())
    if branch_id.isdigit():
        periods_query = periods_query.filter(PayrollPeriod.branch_id == int(branch_id))
    periods = periods_query.all()
    selected = db.query(PayrollPeriod).filter(PayrollPeriod.id == int(period_id)).first() if period_id.isdigit() else (periods[0] if periods else None)
    calculations = (
        db.query(PayrollCalculation)
        .join(HREmployee, HREmployee.id == PayrollCalculation.employee_id)
        .outerjoin(HRArea, HRArea.id == HREmployee.area_id)
        .filter(PayrollCalculation.period_id == selected.id, HREmployee.status == "ACTIVE")
        .order_by(HRArea.name, HREmployee.full_name)
        .all()
        if selected else []
    )
    area_groups = {}
    for calc in calculations:
        area_name = calc.employee.area.name if calc.employee.area else "Sin area"
        area_groups.setdefault(area_name, []).append(calc)
    deduction_lines = (
        db.query(PayrollCalculationDeduction)
        .join(PayrollCalculation)
        .filter(PayrollCalculation.period_id == selected.id, PayrollCalculationDeduction.amount > 0).all()
        if selected else []
    )
    adjustments = (
        db.query(PayrollAdjustment)
        .join(HREmployee, HREmployee.id == PayrollAdjustment.employee_id)
        .filter(PayrollAdjustment.period_id == selected.id, HREmployee.status == "ACTIVE")
        .order_by(PayrollAdjustment.created_at.desc()).all()
        if selected else []
    )
    debts = db.query(PayrollEmployeeDeduction).join(HREmployee).filter(HREmployee.status == "ACTIVE")
    if branch_id.isdigit():
        debts = debts.filter(HREmployee.branch_id == int(branch_id))
    debt_rows = []
    for debt in debts.order_by(HREmployee.full_name).all():
        paid_lines = db.query(PayrollCalculationDeduction).join(PayrollCalculation).join(PayrollPeriod).filter(PayrollCalculationDeduction.employee_deduction_id == debt.id, PayrollCalculationDeduction.amount > 0, PayrollPeriod.status == "CLOSED").order_by(PayrollPeriod.date_from).all()
        debt_rows.append({"debt": debt, "lines": paid_lines, **_debt_schedule(db, debt)})
    report_summary = {
        "employees": len(calculations),
        "gross": _money(sum((_money(row.gross_pay) for row in calculations), Decimal("0"))),
        "deductions": _money(sum((_money(row.total_deductions) for row in calculations), Decimal("0"))),
        "net": _money(sum((_money(row.net_pay) for row in calculations), Decimal("0"))),
        "overtime_minutes": sum((row.overtime_minutes or 0 for row in calculations), 0),
    }
    return request.app.state.templates.TemplateResponse("payroll_reports.html", {"request": request, "user": user, "branches": branches, "periods": periods, "selected_period": selected, "area_groups": area_groups, "calculations": calculations, "deduction_lines": deduction_lines, "adjustments": adjustments, "debt_rows": debt_rows, "report_summary": report_summary})


@router.get("/payroll/periods/{period_id}/report.html")
def payroll_period_html(period_id: int, request: Request, db: Session = Depends(get_db)):
    _browser_admin(request, db)
    period = db.query(PayrollPeriod).filter(PayrollPeriod.id == period_id).first()
    if not period:
        raise HTTPException(404, "Periodo de planilla no encontrado")
    calculations = (
        db.query(PayrollCalculation)
        .join(HREmployee, HREmployee.id == PayrollCalculation.employee_id)
        .outerjoin(HRArea, HRArea.id == HREmployee.area_id)
        .filter(PayrollCalculation.period_id == period.id, HREmployee.status == "ACTIVE")
        .order_by(HRArea.name, HREmployee.full_name)
        .all()
    )
    if not calculations:
        return RedirectResponse(f"/payroll/reports?period_id={period.id}&error=Calcule+la+planilla+antes+de+abrir+el+reporte", status_code=303)
    area_groups = {}
    for calculation in calculations:
        area_name = calculation.employee.area.name if calculation.employee.area else "Sin área"
        area_groups.setdefault(area_name, []).append(calculation)
    summary = {
        "employees": len(calculations),
        "base": _money(sum((_money(row.base_pay) for row in calculations), Decimal("0"))),
        "additions": _money(sum((_money(row.additions_pay) + _money(row.overtime_pay) + _money(row.holiday_pay) for row in calculations), Decimal("0"))),
        "deductions": _money(sum((_money(row.total_deductions) for row in calculations), Decimal("0"))),
        "net": _money(sum((_money(row.net_pay) for row in calculations), Decimal("0"))),
    }
    return request.app.state.templates.TemplateResponse(
        "payroll_period_report.html",
        {
            "request": request,
            "period": period,
            "area_groups": area_groups,
            "summary": summary,
            "generated_at": datetime.now(),
            "branding": getattr(request.state, "branding", {}) or {},
            "format_money": _format_money,
        },
    )


@router.get("/payroll/periods/{period_id}/report.pdf")
def payroll_period_pdf(period_id: int, request: Request, db: Session = Depends(get_db)):
    _browser_admin(request, db)
    period = db.query(PayrollPeriod).filter(PayrollPeriod.id == period_id).first()
    if not period:
        raise HTTPException(404, "Periodo de planilla no encontrado")
    calculations = (
        db.query(PayrollCalculation)
        .join(HREmployee, HREmployee.id == PayrollCalculation.employee_id)
        .filter(PayrollCalculation.period_id == period.id, HREmployee.status == "ACTIVE")
        .order_by(HREmployee.area_id, HREmployee.full_name)
        .all()
    )
    if not calculations:
        return RedirectResponse(f"/payroll/reports?period_id={period.id}&error=Calcule+la+planilla+antes+de+imprimir", status_code=303)

    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    buffer = BytesIO()
    page_size = landscape(A4)
    doc = SimpleDocTemplate(buffer, pagesize=page_size, leftMargin=10 * mm, rightMargin=10 * mm, topMargin=10 * mm, bottomMargin=12 * mm, title=f"Planilla {period.code}", author="Hollywood Pacas")
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="PayrollTitle", parent=styles["Title"], fontSize=16, leading=19, alignment=TA_CENTER, textColor=colors.HexColor("#172554")))
    styles.add(ParagraphStyle(name="PayrollSmall", parent=styles["Normal"], fontSize=7, leading=9))
    styles.add(ParagraphStyle(name="PayrollRight", parent=styles["Normal"], fontSize=7, leading=9, alignment=TA_RIGHT))
    styles.add(ParagraphStyle(name="PayrollMeta", parent=styles["Normal"], fontSize=8, leading=11, textColor=colors.HexColor("#475569")))
    branch_name = period.branch.name if period.branch else "Sucursal sin asignar"
    company_title = f"HOLLYWOOD PACAS - {branch_name.upper()}"
    target_days = (period.date_to - period.date_from).days + 1
    calculated_at = max((row.calculated_at for row in calculations if row.calculated_at), default=datetime.now())
    status_label = "CERRADA" if period.status == "CLOSED" else "BORRADOR / ACUMULADO"
    header = Table(
        [[Paragraph(company_title, styles["PayrollTitle"]), Paragraph(f"<b>{status_label}</b><br/><font size='8'>Planilla {period.code}</font>", styles["PayrollRight"])]],
        colWidths=[215 * mm, 62 * mm],
    )
    header.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eff6ff")), ("BOX", (0, 0), (-1, -1), .8, colors.HexColor("#1d4ed8")), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 7), ("BOTTOMPADDING", (0, 0), (-1, -1), 7)]))
    period_meta = Table(
        [["PERIODO", "FECHA DE PAGO", "CORTE DEL CALCULO", "COLABORADORES"], [f"{period.date_from:%d/%m/%Y} al {period.date_to:%d/%m/%Y}", f"{period.pay_date:%d/%m/%Y}", f"{calculated_at:%d/%m/%Y %I:%M %p}", str(len(calculations))]],
        colWidths=[78 * mm, 61 * mm, 78 * mm, 60 * mm],
    )
    period_meta.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#172554")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 8), ("ALIGN", (0, 0), (-1, -1), "CENTER"), ("BOX", (0, 0), (-1, -1), .5, colors.HexColor("#94a3b8")), ("INNERGRID", (0, 0), (-1, -1), .25, colors.HexColor("#cbd5e1")), ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
    story = [header, Spacer(1, 3 * mm), period_meta, Spacer(1, 5 * mm)]

    groups = {}
    for calc in calculations:
        groups.setdefault(calc.employee.area.name if calc.employee.area else "Sin area", []).append(calc)
    grand = {"base": Decimal("0"), "add": Decimal("0"), "extra": Decimal("0"), "holiday": Decimal("0"), "ded": Decimal("0"), "net": Decimal("0")}
    widths = [50*mm, 14*mm, 31*mm, 28*mm, 22*mm, 30*mm, 28*mm, 32*mm, 42*mm]
    for area, rows in groups.items():
        area_header = Table([[f"AREA: {area.upper()}", f"{len(rows)} empleado(s)"]], colWidths=[220 * mm, 57 * mm])
        area_header.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#dbeafe")), ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#172554")), ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"), ("ALIGN", (1, 0), (1, 0), "RIGHT"), ("BOTTOMPADDING", (0, 0), (-1, -1), 5), ("TOPPADDING", (0, 0), (-1, -1), 5)]))
        story.append(area_header)
        data = [["Empleado / codigo", "Dias", "Base acumulada", "Adiciones", "H. extra", "Pago extra", "Feriados", "Deducciones", "Neto a recibir"]]
        area_total = {key: Decimal("0") for key in grand}
        for row in rows:
            overtime_hours = Decimal(row.overtime_minutes or 0) / Decimal("60")
            employee_label = f"<b>{row.employee.full_name}</b><br/><font color='#64748b'>{row.employee.employee_code}</font>"
            data.append([Paragraph(employee_label, styles["PayrollSmall"]), f"{row.days_worked}/{target_days}", f"C$ {_money(row.base_pay):,.2f}", f"C$ {_money(row.additions_pay):,.2f}", f"{overtime_hours:.2f} h", f"C$ {_money(row.overtime_pay):,.2f}", f"C$ {_money(row.holiday_pay):,.2f}", f"C$ {_money(row.total_deductions):,.2f}", f"C$ {_money(row.net_pay):,.2f}"])
            for key, value in (("base", row.base_pay), ("add", row.additions_pay), ("extra", row.overtime_pay), ("holiday", row.holiday_pay), ("ded", row.total_deductions), ("net", row.net_pay)):
                area_total[key] += _money(value)
                grand[key] += _money(value)
        data.append(["TOTAL AREA", "", f"C$ {area_total['base']:,.2f}", f"C$ {area_total['add']:,.2f}", "", f"C$ {area_total['extra']:,.2f}", f"C$ {area_total['holiday']:,.2f}", f"C$ {area_total['ded']:,.2f}", f"C$ {area_total['net']:,.2f}"])
        table = Table(data, colWidths=widths, repeatRows=1)
        table.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor("#172554")), ("TEXTCOLOR", (0,0), (-1,0), colors.white), ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"), ("BACKGROUND", (0,-1), (-1,-1), colors.HexColor("#e2e8f0")), ("FONTNAME", (0,-1), (-1,-1), "Helvetica-Bold"), ("GRID", (0,0), (-1,-1), .35, colors.HexColor("#94a3b8")), ("FONTSIZE", (0,0), (-1,-1), 6.7), ("ALIGN", (1,1), (-1,-1), "RIGHT"), ("VALIGN", (0,0), (-1,-1), "MIDDLE"), ("ROWBACKGROUNDS", (0,1), (-1,-2), [colors.white, colors.HexColor("#f8fafc")]), ("TOPPADDING", (0,0), (-1,-1), 4), ("BOTTOMPADDING", (0,0), (-1,-1), 4)]))
        story.extend([table, Spacer(1, 4 * mm)])

    total_extra_hours = Decimal(sum((row.overtime_minutes or 0 for row in calculations), 0)) / Decimal("60")
    summary = Table([["RESUMEN GENERAL", "Base acumulada", "Adiciones", "Horas extra", "Feriados", "Deducciones", "NETO TOTAL"], [branch_name, f"C$ {grand['base']:,.2f}", f"C$ {grand['add']:,.2f}", f"{total_extra_hours:.2f} h / C$ {grand['extra']:,.2f}", f"C$ {grand['holiday']:,.2f}", f"C$ {grand['ded']:,.2f}", f"C$ {grand['net']:,.2f}"]], colWidths=[53*mm, 35*mm, 32*mm, 47*mm, 31*mm, 35*mm, 44*mm])
    summary.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor("#10233f")), ("TEXTCOLOR", (0,0), (-1,0), colors.white), ("BACKGROUND", (0,1), (-1,1), colors.HexColor("#eef4ff")), ("FONTNAME", (0,0), (-1,-1), "Helvetica-Bold"), ("GRID", (0,0), (-1,-1), .5, colors.HexColor("#94a3b8")), ("FONTSIZE", (0,0), (-1,-1), 8), ("ALIGN", (1,1), (-1,-1), "RIGHT"), ("TOPPADDING", (0,0), (-1,-1), 6), ("BOTTOMPADDING", (0,0), (-1,-1), 6)]))
    story.extend([summary, Spacer(1, 13 * mm), Table([["______________________________", "______________________________", "______________________________"], ["Elaborado por", "Revisado por", "Autorizado por"]], colWidths=[92.33*mm, 92.33*mm, 92.34*mm], style=TableStyle([("ALIGN", (0,0), (-1,-1), "CENTER"), ("FONTSIZE", (0,0), (-1,-1), 8)]))])

    def footer(canvas, document):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#d9e1ea"))
        canvas.roundRect(6 * mm, 6 * mm, page_size[0] - 12 * mm, page_size[1] - 12 * mm, 2 * mm, stroke=1, fill=0)
        canvas.setStrokeColor(colors.HexColor("#cbd5e1"))
        canvas.line(10 * mm, 10 * mm, page_size[0] - 10 * mm, 10 * mm)
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(colors.HexColor("#475569"))
        canvas.drawString(10 * mm, 6 * mm, "Hollywood Pacas · Documento de control interno de planilla")
        canvas.drawRightString(page_size[0] - 10 * mm, 7 * mm, f"Pagina {document.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    buffer.seek(0)
    filename = f"planilla_{period.branch.code if period.branch else 'sucursal'}_{period.date_from:%Y%m%d}_{period.date_to:%Y%m%d}.pdf"
    return StreamingResponse(buffer, media_type="application/pdf", headers={"Content-Disposition": f'inline; filename="{filename}"'})


@router.get("/payroll/settlements")
def payroll_settlements(request: Request, branch_id: str = "", as_of: str = "", db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    try:
        cutoff = date.fromisoformat(as_of) if as_of else date.today()
    except ValueError:
        cutoff = date.today()
    query = db.query(PayrollEmployeeProfile).join(HREmployee).filter(PayrollEmployeeProfile.active.is_(True), HREmployee.status == "ACTIVE")
    if branch_id.isdigit():
        query = query.filter(HREmployee.branch_id == int(branch_id))
    accruals = [{"profile": profile, "employee": profile.employee, **_benefit_preview(profile, profile.employee, cutoff)} for profile in query.order_by(HREmployee.full_name).all()]
    settlements = db.query(PayrollSettlement).order_by(PayrollSettlement.created_at.desc()).limit(100).all()
    return request.app.state.templates.TemplateResponse("payroll_settlements.html", {"request": request, "user": user, "branches": db.query(Branch).filter(Branch.activo.is_(True)).order_by(Branch.name).all(), "accruals": accruals, "settlements": settlements, "as_of": cutoff})


@router.post("/payroll/settlements")
def create_settlement(request: Request, employee_id: int = Form(...), termination_date: date = Form(...), reason_code: str = Form(...), reason_detail: str = Form(...), apply_seniority: Optional[str] = Form(None), other_additions: Decimal = Form(0), manual_deductions: Decimal = Form(0), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    profile = db.query(PayrollEmployeeProfile).filter(PayrollEmployeeProfile.employee_id == employee_id, PayrollEmployeeProfile.active.is_(True)).first()
    if not profile or not profile.employee.branch_id:
        return RedirectResponse("/payroll/settlements?error=Empleado+sin+perfil+o+sucursal", status_code=303)
    if db.query(PayrollSettlement).filter(PayrollSettlement.employee_id == employee_id, PayrollSettlement.status.in_(["DRAFT", "PAID"])).first():
        return RedirectResponse("/payroll/settlements?error=El+empleado+ya+tiene+una+liquidacion+activa", status_code=303)
    preview = _benefit_preview(profile, profile.employee, termination_date)
    if not preview["start"] or termination_date < preview["start"]:
        return RedirectResponse("/payroll/settlements?error=Revise+la+fecha+de+ingreso", status_code=303)
    use_seniority = apply_seniority == "on"
    enabled_ids = {row.deduction_type_id for row in db.query(PayrollEmployeeDeductionSetting).filter(PayrollEmployeeDeductionSetting.employee_id == employee_id, PayrollEmployeeDeductionSetting.enabled.is_(True)).all()}
    scheduled = Decimal("0")
    if enabled_ids:
        for debt in db.query(PayrollEmployeeDeduction).filter(PayrollEmployeeDeduction.employee_id == employee_id, PayrollEmployeeDeduction.deduction_type_id.in_(enabled_ids), PayrollEmployeeDeduction.status == "ACTIVE", PayrollEmployeeDeduction.start_date <= termination_date).all():
            paid = _money(db.query(func.sum(PayrollCalculationDeduction.amount)).join(PayrollCalculation, PayrollCalculation.id == PayrollCalculationDeduction.calculation_id).join(PayrollPeriod, PayrollPeriod.id == PayrollCalculation.period_id).filter(PayrollCalculationDeduction.employee_deduction_id == debt.id, PayrollPeriod.status == "CLOSED").scalar())
            remaining = max(Decimal("0"), _money(debt.original_amount) - paid)
            scheduled += min(remaining, _money(debt.installment_amount))
    settlement_seniority_days = max(Decimal("30"), preview["seniority_days"]) if use_seniority and preview["service_days"] > 0 else Decimal("0")
    seniority_amount = _money(_money(profile.monthly_salary) / 30 * settlement_seniority_days)
    additions = _money(other_additions)
    gross = _money(preview["vacation_amount"] + preview["bonus_amount"] + seniority_amount + additions)
    deductions = min(gross, _money(scheduled + _money(manual_deductions)))
    row = PayrollSettlement(employee_id=employee_id, branch_id=profile.employee.branch_id, termination_date=termination_date, reason_code=reason_code, reason_detail=reason_detail.strip(), salary_snapshot=_money(profile.monthly_salary), service_days=preview["service_days"], vacation_days=preview["vacation_days"], vacation_amount=preview["vacation_amount"], bonus_days=preview["bonus_days"], bonus_amount=preview["bonus_amount"], seniority_days=settlement_seniority_days, seniority_amount=seniority_amount, other_additions=additions, deductions_amount=deductions, gross_amount=gross, net_amount=_money(gross - deductions), apply_seniority=use_seniority, created_by=user.email)
    db.add(row)
    db.commit()
    return RedirectResponse("/payroll/settlements?ok=Liquidacion+calculada+en+borrador", status_code=303)


@router.post("/payroll/settlements/{settlement_id}/pay")
def pay_settlement(settlement_id: int, request: Request, db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    row = db.query(PayrollSettlement).filter(PayrollSettlement.id == settlement_id, PayrollSettlement.status == "DRAFT").first()
    if not row:
        return RedirectResponse("/payroll/settlements?error=Liquidacion+no+disponible", status_code=303)
    profile = db.query(PayrollEmployeeProfile).filter(PayrollEmployeeProfile.employee_id == row.employee_id).first()
    row.status, row.paid_by, row.paid_at = "PAID", user.email, datetime.utcnow()
    if profile:
        profile.vacation_paid_through = row.termination_date
        profile.bonus_paid_through = row.termination_date
        if row.apply_seniority:
            profile.seniority_paid_through = row.termination_date
        profile.active = False
    row.employee.status, row.employee.termination_date = "TERMINATED", row.termination_date
    db.commit()
    return RedirectResponse("/payroll/settlements?ok=Liquidacion+pagada+y+acumulados+cerrados", status_code=303)


@router.post("/payroll/settlements/{settlement_id}/void")
def void_settlement(settlement_id: int, request: Request, reason: str = Form(...), db: Session = Depends(get_db)):
    user = _browser_admin(request, db)
    row = db.query(PayrollSettlement).filter(PayrollSettlement.id == settlement_id, PayrollSettlement.status == "DRAFT").first()
    if not row:
        return RedirectResponse("/payroll/settlements?error=Solo+se+anulan+borradores", status_code=303)
    row.status, row.void_reason, row.voided_by, row.voided_at = "VOID", reason.strip(), user.email, datetime.utcnow()
    db.commit()
    return RedirectResponse("/payroll/settlements?ok=Liquidacion+anulada", status_code=303)
