"""add final employment settlements

Revision ID: 7b8c9d0e1f2a
Revises: 6a7b8c9d0e1f
"""
import sqlalchemy as sa
from alembic import op

revision = "7b8c9d0e1f2a"
down_revision = "6a7b8c9d0e1f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("payroll_employee_profiles", sa.Column("vacation_paid_through", sa.Date()))
    op.add_column("payroll_employee_profiles", sa.Column("bonus_paid_through", sa.Date()))
    op.add_column("payroll_employee_profiles", sa.Column("seniority_paid_through", sa.Date()))
    op.create_table("payroll_settlements", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id"), nullable=False), sa.Column("branch_id", sa.Integer(), sa.ForeignKey("branches.id"), nullable=False), sa.Column("termination_date", sa.Date(), nullable=False), sa.Column("reason_code", sa.String(40), nullable=False), sa.Column("reason_detail", sa.String(300), nullable=False), sa.Column("salary_snapshot", sa.Numeric(14,2), nullable=False), sa.Column("service_days", sa.Integer(), nullable=False), sa.Column("vacation_days", sa.Numeric(10,4), nullable=False), sa.Column("vacation_amount", sa.Numeric(14,2), nullable=False), sa.Column("bonus_days", sa.Numeric(10,4), nullable=False), sa.Column("bonus_amount", sa.Numeric(14,2), nullable=False), sa.Column("seniority_days", sa.Numeric(10,4), nullable=False), sa.Column("seniority_amount", sa.Numeric(14,2), nullable=False), sa.Column("other_additions", sa.Numeric(14,2), nullable=False, server_default="0"), sa.Column("deductions_amount", sa.Numeric(14,2), nullable=False, server_default="0"), sa.Column("gross_amount", sa.Numeric(14,2), nullable=False), sa.Column("net_amount", sa.Numeric(14,2), nullable=False), sa.Column("apply_seniority", sa.Boolean(), nullable=False, server_default=sa.true()), sa.Column("status", sa.String(20), nullable=False, server_default="DRAFT"), sa.Column("created_by", sa.String(160)), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()), sa.Column("paid_by", sa.String(160)), sa.Column("paid_at", sa.DateTime()), sa.Column("void_reason", sa.String(240)), sa.Column("voided_by", sa.String(160)), sa.Column("voided_at", sa.DateTime()))


def downgrade() -> None:
    op.drop_table("payroll_settlements")
    op.drop_column("payroll_employee_profiles", "seniority_paid_through")
    op.drop_column("payroll_employee_profiles", "bonus_paid_through")
    op.drop_column("payroll_employee_profiles", "vacation_paid_through")
