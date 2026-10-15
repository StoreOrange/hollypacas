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

    accounting_account_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=True)
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

    accounting_entry_id = Column(Integer, ForeignKey('accounting_entries.id'), nullable=True)
    accounting_entry = relationship('AccountingEntry', foreign_keys=[accounting_entry_id])
    accounting_payable_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=True)
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
    __table_args__ = (UniqueConstraint("branch_id", "holiday_date", name="uq_payroll_holiday_branch_date"),)

    id = Column(Integer, primary_key=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=True)
    holiday_date = Column(Date, nullable=False)
    name = Column(String(160), nullable=False)
    period_id = Column(Integer, ForeignKey("payroll_periods.id"), nullable=True)
    paid = Column(Boolean, nullable=False, default=True)
    worked_as_overtime = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    period = relationship("PayrollPeriod")
    branch = relationship("Branch")


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
    depends_on_id = Column(Integer, ForeignKey("payroll_employee_deductions.id"), nullable=True)
    status = Column(String(20), nullable=False, default="ACTIVE")
    paused_until = Column(Date, nullable=True)
    pause_reason = Column(String(240), nullable=True)
    paused_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    updated_by = Column(String(160), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    employee = relationship("HREmployee")
    deduction_type = relationship("PayrollDeductionType")
    depends_on = relationship("PayrollEmployeeDeduction", remote_side=[id], foreign_keys=[depends_on_id])


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
    late_minutes = Column(Integer, nullable=False, default=0)
    late_deduction = Column(Numeric(14, 2), nullable=False, default=0)
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

    accounting_entry_id = Column(Integer, ForeignKey('accounting_entries.id'), nullable=True)
    accounting_entry = relationship('AccountingEntry', foreign_keys=[accounting_entry_id])
    reversal_entry_id = Column(Integer, ForeignKey('accounting_entries.id'), nullable=True)
    reversal_entry = relationship('AccountingEntry', foreign_keys=[reversal_entry_id])
    cash_receipt_id = Column(Integer, ForeignKey('recibos_caja.id'), nullable=True)
    refund_receipt_id = Column(Integer, ForeignKey('recibos_caja.id'), nullable=True)
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


class PayrollAccountingSetting(Base):
    __tablename__ = 'payroll_accounting_settings'
    id = Column(Integer, primary_key=True)
    salary_expense_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=False)
    salary_payable_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=False)
    manual_deduction_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=True)
    updated_by = Column(String(160), nullable=True)
    updated_at = Column(DateTime, nullable=True)


class PayrollBenefitSetting(Base):
    __tablename__ = 'payroll_benefit_settings'
    id = Column(Integer, primary_key=True)
    enabled = Column(Boolean, nullable=False, default=False)
    start_date = Column(Date, nullable=False)
    expense_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=False)
    opening_debit_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=False)
    vacation_payable_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=False)
    bonus_payable_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=False)
    seniority_payable_id = Column(Integer, ForeignKey('cuentas_contables.id'), nullable=False)
    updated_by = Column(String(160), nullable=True)
    last_error = Column(Text, nullable=True)
    last_run = Column(DateTime, nullable=True)


class PayrollBenefitAccrual(Base):
    __tablename__ = 'payroll_benefit_accruals'
    __table_args__ = (UniqueConstraint('employee_id', 'as_of', name='uq_payroll_benefit_day'),)
    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('hr_employees.id'), nullable=False)
    as_of = Column(Date, nullable=False)
    salary = Column(Numeric(14, 2), nullable=False)
    vacation_amount = Column(Numeric(14, 2), nullable=False)
    bonus_amount = Column(Numeric(14, 2), nullable=False)
    seniority_amount = Column(Numeric(14, 2), nullable=False)
    accounting_entry_id = Column(Integer, ForeignKey('accounting_entries.id'), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    employee = relationship('HREmployee')


class PayrollVacationMovement(Base):
    __tablename__ = 'payroll_vacation_movements'
    id = Column(Integer, primary_key=True)
    operation_key = Column(String(32), unique=True, nullable=False)
    employee_id = Column(Integer, ForeignKey('hr_employees.id'), nullable=False)
    kind = Column(String(20), nullable=False)  # REST / CASH / SETTLEMENT
    effective_date = Column(Date, nullable=False)
    date_from = Column(Date, nullable=True)
    date_to = Column(Date, nullable=True)
    days = Column(Numeric(10, 4), nullable=False)
    salary = Column(Numeric(14, 2), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    reason = Column(String(300), nullable=False)
    status = Column(String(20), nullable=False, default='ACTIVE')
    settlement_id = Column(Integer, ForeignKey('payroll_settlements.id'), unique=True, nullable=True)
    accounting_entry_id = Column(Integer, ForeignKey('accounting_entries.id'), nullable=True)
    cash_receipt_id = Column(Integer, ForeignKey('recibos_caja.id'), nullable=True)
    created_by = Column(String(160), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    employee = relationship('HREmployee')


class PayrollVacationRelease(Base):
    __tablename__ = 'payroll_vacation_releases'
    __table_args__ = (UniqueConstraint('movement_id', 'period_id', name='uq_vacation_period_release'),)
    id = Column(Integer, primary_key=True)
    movement_id = Column(Integer, ForeignKey('payroll_vacation_movements.id'), nullable=False)
    period_id = Column(Integer, ForeignKey('payroll_periods.id'), nullable=False)
    days = Column(Numeric(10, 4), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    accounting_entry_id = Column(Integer, ForeignKey('accounting_entries.id'), nullable=True)


class PayrollSettlementDeduction(Base):
    __tablename__ = 'payroll_settlement_deductions'
    __table_args__ = (UniqueConstraint('settlement_id','employee_deduction_id',name='uq_settlement_debt'),)
    id = Column(Integer, primary_key=True)
    settlement_id = Column(Integer, ForeignKey('payroll_settlements.id'), nullable=False)
    employee_deduction_id = Column(Integer, ForeignKey('payroll_employee_deductions.id'), nullable=False)
    amount = Column(Numeric(14,2), nullable=False)
    employee_deduction = relationship('PayrollEmployeeDeduction')


class PayrollVacationReversal(Base):
    __tablename__ = 'payroll_vacation_reversals'
    id = Column(Integer, primary_key=True)
    movement_id = Column(Integer, ForeignKey('payroll_vacation_movements.id'), unique=True, nullable=False)
    reason = Column(String(300), nullable=False)
    accounting_entry_id = Column(Integer, ForeignKey('accounting_entries.id'), nullable=True)
    cash_receipt_id = Column(Integer, ForeignKey('recibos_caja.id'), nullable=True)
    created_by = Column(String(160), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
