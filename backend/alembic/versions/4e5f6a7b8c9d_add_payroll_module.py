"""add payroll management module

Revision ID: 4e5f6a7b8c9d
Revises: 3d4e5f6a7b8c
"""

import sqlalchemy as sa
from alembic import op

revision = "4e5f6a7b8c9d"
down_revision = "3d4e5f6a7b8c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("payroll_deduction_types", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("code", sa.String(40), nullable=False, unique=True), sa.Column("name", sa.String(120), nullable=False, unique=True), sa.Column("category", sa.String(30), nullable=False, server_default="OTHER"), sa.Column("is_loan", sa.Boolean(), nullable=False, server_default=sa.false()), sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()))
    op.create_table("payroll_periods", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("code", sa.String(40), nullable=False, unique=True), sa.Column("date_from", sa.Date(), nullable=False), sa.Column("date_to", sa.Date(), nullable=False), sa.Column("pay_date", sa.Date(), nullable=False), sa.Column("status", sa.String(20), nullable=False, server_default="DRAFT"), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()), sa.Column("closed_at", sa.DateTime()), sa.UniqueConstraint("date_from", "date_to", name="uq_payroll_period_range"))
    op.create_table("payroll_employee_profiles", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id", ondelete="CASCADE"), nullable=False, unique=True), sa.Column("monthly_salary", sa.Numeric(14, 2), nullable=False, server_default="0"), sa.Column("currency", sa.String(10), nullable=False, server_default="NIO"), sa.Column("pay_frequency", sa.String(20), nullable=False, server_default="QUINCENAL"), sa.Column("contract_start", sa.Date()), sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()), sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()))
    op.create_table("payroll_holidays", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("holiday_date", sa.Date(), nullable=False, unique=True), sa.Column("name", sa.String(160), nullable=False), sa.Column("period_id", sa.Integer(), sa.ForeignKey("payroll_periods.id")), sa.Column("paid", sa.Boolean(), nullable=False, server_default=sa.true()), sa.Column("worked_as_overtime", sa.Boolean(), nullable=False, server_default=sa.true()), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()))
    op.create_table("payroll_employee_deductions", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id"), nullable=False), sa.Column("deduction_type_id", sa.Integer(), sa.ForeignKey("payroll_deduction_types.id"), nullable=False), sa.Column("description", sa.String(200), nullable=False), sa.Column("original_amount", sa.Numeric(14, 2), nullable=False), sa.Column("installment_count", sa.Integer(), nullable=False, server_default="1"), sa.Column("installment_amount", sa.Numeric(14, 2), nullable=False), sa.Column("start_date", sa.Date(), nullable=False), sa.Column("status", sa.String(20), nullable=False, server_default="ACTIVE"), sa.Column("notes", sa.Text()), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()))
    op.create_table("payroll_calculations", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("period_id", sa.Integer(), sa.ForeignKey("payroll_periods.id", ondelete="CASCADE"), nullable=False), sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id"), nullable=False), sa.Column("monthly_salary", sa.Numeric(14, 2), nullable=False), sa.Column("base_pay", sa.Numeric(14, 2), nullable=False), sa.Column("days_worked", sa.Integer(), nullable=False, server_default="0"), sa.Column("overtime_minutes", sa.Integer(), nullable=False, server_default="0"), sa.Column("overtime_pay", sa.Numeric(14, 2), nullable=False, server_default="0"), sa.Column("holiday_pay", sa.Numeric(14, 2), nullable=False, server_default="0"), sa.Column("gross_pay", sa.Numeric(14, 2), nullable=False), sa.Column("total_deductions", sa.Numeric(14, 2), nullable=False, server_default="0"), sa.Column("net_pay", sa.Numeric(14, 2), nullable=False), sa.Column("status", sa.String(20), nullable=False, server_default="DRAFT"), sa.Column("calculated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()), sa.UniqueConstraint("period_id", "employee_id", name="uq_payroll_period_employee"))
    op.create_table("payroll_calculation_deductions", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("calculation_id", sa.Integer(), sa.ForeignKey("payroll_calculations.id", ondelete="CASCADE"), nullable=False), sa.Column("employee_deduction_id", sa.Integer(), sa.ForeignKey("payroll_employee_deductions.id"), nullable=False), sa.Column("amount", sa.Numeric(14, 2), nullable=False), sa.Column("installment_number", sa.Integer(), nullable=False), sa.UniqueConstraint("calculation_id", "employee_deduction_id", name="uq_payroll_calc_deduction"))


def downgrade() -> None:
    for table in ("payroll_calculation_deductions", "payroll_calculations", "payroll_employee_deductions", "payroll_holidays", "payroll_employee_profiles", "payroll_periods", "payroll_deduction_types"):
        op.drop_table(table)
