from datetime import date, datetime

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship

from ..database import Base


class PayrollEmployeeProfile(Base):
    __tablename__ = "payroll_employee_profiles"

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey("hr_employees.id", ondelete="CASCADE"), unique=True, nullable=False)
    monthly_salary = Column(Numeric(14, 2), nullable=False, default=0)
    currency = Column(String(10), nullable=False, default="NIO")
    pay_frequency = Column(String(20), nullable=False, default="QUINCENAL")
    contract_start = Column(Date, nullable=True)
    vacation_paid_through = Column(Date, nullable=True)
    bonus_paid_through = Column(Date, nullable=True)
    seniority_paid_through = Column(Date, nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    employee = relationship("HREmployee")


class PayrollDeductionType(Base):
    __tablename__ = "payroll_deduction_types"

    id = Column(Integer, primary_key=True)
    code = Column(String(40), unique=True, nullable=False)
    name = Column(String(120), unique=True, nullable=False)
    category = Column(String(30), nullable=False, default="OTHER")
    is_loan = Column(Boolean, nullable=False, default=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class PayrollEmployeeDeductionSetting(Base):
    __tablename__ = "payroll_employee_deduction_settings"
    __table_args__ = (
        UniqueConstraint("employee_id", "deduction_type_id", name="uq_payroll_employee_deduction_setting"),
    )

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey("hr_employees.id", ondelete="CASCADE"), nullable=False)
    deduction_type_id = Column(Integer, ForeignKey("payroll_deduction_types.id", ondelete="CASCADE"), nullable=False)
    enabled = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    employee = relationship("HREmployee")
    deduction_type = relationship("PayrollDeductionType")


class PayrollPeriod(Base):
    __tablename__ = "payroll_periods"
    __table_args__ = (UniqueConstraint("branch_id", "date_from", "date_to", name="uq_payroll_branch_period_range"),)

    id = Column(Integer, primary_key=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=True)
    code = Column(String(40), unique=True, nullable=False)
    date_from = Column(Date, nullable=False)
    date_to = Column(Date, nullable=False)
    pay_date = Column(Date, nullable=False)
    status = Column(String(20), nullable=False, default="DRAFT")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

    branch = relationship("Branch")


class PayrollHoliday(Base):
    __tablename__ = "payroll_holidays"

    id = Column(Integer, primary_key=True)
    holiday_date = Column(Date, unique=True, nullable=False)
    name = Column(String(160), nullable=False)
    period_id = Column(Integer, ForeignKey("payroll_periods.id"), nullable=True)
    paid = Column(Boolean, nullable=False, default=True)
    worked_as_overtime = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    period = relationship("PayrollPeriod")


class PayrollEmployeeDeduction(Base):
    __tablename__ = "payroll_employee_deductions"

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey("hr_employees.id"), nullable=False)
    deduction_type_id = Column(Integer, ForeignKey("payroll_deduction_types.id"), nullable=False)
    description = Column(String(200), nullable=False)
    original_amount = Column(Numeric(14, 2), nullable=False)
    installment_count = Column(Integer, nullable=False, default=1)
    installment_amount = Column(Numeric(14, 2), nullable=False)
    start_date = Column(Date, nullable=False, default=date.today)
    status = Column(String(20), nullable=False, default="ACTIVE")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    employee = relationship("HREmployee")
    deduction_type = relationship("PayrollDeductionType")


class PayrollCalculation(Base):
    __tablename__ = "payroll_calculations"
    __table_args__ = (UniqueConstraint("period_id", "employee_id", name="uq_payroll_period_employee"),)

    id = Column(Integer, primary_key=True)
    period_id = Column(Integer, ForeignKey("payroll_periods.id", ondelete="CASCADE"), nullable=False)
    employee_id = Column(Integer, ForeignKey("hr_employees.id"), nullable=False)
    monthly_salary = Column(Numeric(14, 2), nullable=False)
    base_pay = Column(Numeric(14, 2), nullable=False)
    days_worked = Column(Integer, nullable=False, default=0)
    overtime_minutes = Column(Integer, nullable=False, default=0)
    overtime_pay = Column(Numeric(14, 2), nullable=False, default=0)
    holiday_pay = Column(Numeric(14, 2), nullable=False, default=0)
    additions_pay = Column(Numeric(14, 2), nullable=False, default=0)
    gross_pay = Column(Numeric(14, 2), nullable=False)
    total_deductions = Column(Numeric(14, 2), nullable=False, default=0)
    net_pay = Column(Numeric(14, 2), nullable=False)
    status = Column(String(20), nullable=False, default="DRAFT")
    calculated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    period = relationship("PayrollPeriod")
    employee = relationship("HREmployee")
    deduction_lines = relationship("PayrollCalculationDeduction", back_populates="calculation", cascade="all, delete-orphan")


class PayrollAdjustment(Base):
    __tablename__ = "payroll_adjustments"

    id = Column(Integer, primary_key=True)
    period_id = Column(Integer, ForeignKey("payroll_periods.id", ondelete="CASCADE"), nullable=False)
    employee_id = Column(Integer, ForeignKey("hr_employees.id"), nullable=False)
    adjustment_type = Column(String(20), nullable=False)  # ADDITION / DEDUCTION / WORKED_DAYS
    description = Column(String(200), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False, default=0)
    worked_days = Column(Integer, nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    created_by = Column(String(160), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    void_reason = Column(String(240), nullable=True)
    voided_by = Column(String(160), nullable=True)
    voided_at = Column(DateTime, nullable=True)

    period = relationship("PayrollPeriod")
    employee = relationship("HREmployee")


class PayrollPayment(Base):
    __tablename__ = "payroll_payments"

    id = Column(Integer, primary_key=True)
    period_id = Column(Integer, ForeignKey("payroll_periods.id"), nullable=False)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    payment_date = Column(Date, nullable=False, default=date.today)
    payment_method = Column(String(40), nullable=False, default="EFECTIVO")
    reference = Column(String(120), nullable=True)
    status = Column(String(20), nullable=False, default="PAID")
    created_by = Column(String(160), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    void_reason = Column(String(240), nullable=True)
    voided_by = Column(String(160), nullable=True)
    voided_at = Column(DateTime, nullable=True)

    period = relationship("PayrollPeriod")
    branch = relationship("Branch")


class PayrollSettlement(Base):
    __tablename__ = "payroll_settlements"

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey("hr_employees.id"), nullable=False)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    termination_date = Column(Date, nullable=False)
    reason_code = Column(String(40), nullable=False)
    reason_detail = Column(String(300), nullable=False)
    salary_snapshot = Column(Numeric(14, 2), nullable=False)
    service_days = Column(Integer, nullable=False, default=0)
    vacation_days = Column(Numeric(10, 4), nullable=False, default=0)
    vacation_amount = Column(Numeric(14, 2), nullable=False, default=0)
    bonus_days = Column(Numeric(10, 4), nullable=False, default=0)
    bonus_amount = Column(Numeric(14, 2), nullable=False, default=0)
    seniority_days = Column(Numeric(10, 4), nullable=False, default=0)
    seniority_amount = Column(Numeric(14, 2), nullable=False, default=0)
    other_additions = Column(Numeric(14, 2), nullable=False, default=0)
    deductions_amount = Column(Numeric(14, 2), nullable=False, default=0)
    gross_amount = Column(Numeric(14, 2), nullable=False)
    net_amount = Column(Numeric(14, 2), nullable=False)
    apply_seniority = Column(Boolean, nullable=False, default=True)
    status = Column(String(20), nullable=False, default="DRAFT")
    created_by = Column(String(160), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    paid_by = Column(String(160), nullable=True)
    paid_at = Column(DateTime, nullable=True)
    void_reason = Column(String(240), nullable=True)
    voided_by = Column(String(160), nullable=True)
    voided_at = Column(DateTime, nullable=True)

    employee = relationship("HREmployee")
    branch = relationship("Branch")


class PayrollCalculationDeduction(Base):
    __tablename__ = "payroll_calculation_deductions"
    __table_args__ = (UniqueConstraint("calculation_id", "employee_deduction_id", name="uq_payroll_calc_deduction"),)

    id = Column(Integer, primary_key=True)
    calculation_id = Column(Integer, ForeignKey("payroll_calculations.id", ondelete="CASCADE"), nullable=False)
    employee_deduction_id = Column(Integer, ForeignKey("payroll_employee_deductions.id"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    installment_number = Column(Integer, nullable=False)

    calculation = relationship("PayrollCalculation", back_populates="deduction_lines")
    employee_deduction = relationship("PayrollEmployeeDeduction")


class PayrollDeductionOverride(Base):
    __tablename__ = "payroll_deduction_overrides"
    __table_args__ = (UniqueConstraint("period_id", "employee_deduction_id", name="uq_payroll_period_deduction_override"),)

    id = Column(Integer, primary_key=True)
    period_id = Column(Integer, ForeignKey("payroll_periods.id", ondelete="CASCADE"), nullable=False)
    employee_deduction_id = Column(Integer, ForeignKey("payroll_employee_deductions.id", ondelete="CASCADE"), nullable=False)
    apply_charge = Column(Boolean, nullable=False, default=True)
    override_amount = Column(Numeric(14, 2), nullable=True)
    reason = Column(String(240), nullable=False)
    created_by = Column(String(160), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    period = relationship("PayrollPeriod")
    employee_deduction = relationship("PayrollEmployeeDeduction")
