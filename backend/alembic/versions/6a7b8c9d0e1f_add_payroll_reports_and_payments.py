"""add payroll branches reports adjustments and payments

Revision ID: 6a7b8c9d0e1f
Revises: 5f6a7b8c9d0e
"""
import sqlalchemy as sa
from alembic import op

revision = "6a7b8c9d0e1f"
down_revision = "5f6a7b8c9d0e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("payroll_periods", sa.Column("branch_id", sa.Integer(), nullable=True))
    op.create_foreign_key("fk_payroll_period_branch", "payroll_periods", "branches", ["branch_id"], ["id"])
    op.drop_constraint("uq_payroll_period_range", "payroll_periods", type_="unique")
    op.create_unique_constraint("uq_payroll_branch_period_range", "payroll_periods", ["branch_id", "date_from", "date_to"])
    op.add_column("payroll_calculations", sa.Column("additions_pay", sa.Numeric(14, 2), nullable=False, server_default="0"))
    op.create_table("payroll_adjustments", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("period_id", sa.Integer(), sa.ForeignKey("payroll_periods.id", ondelete="CASCADE"), nullable=False), sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id"), nullable=False), sa.Column("adjustment_type", sa.String(20), nullable=False), sa.Column("description", sa.String(200), nullable=False), sa.Column("amount", sa.Numeric(14, 2), nullable=False, server_default="0"), sa.Column("worked_days", sa.Integer()), sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()), sa.Column("created_by", sa.String(160)), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()), sa.Column("void_reason", sa.String(240)), sa.Column("voided_by", sa.String(160)), sa.Column("voided_at", sa.DateTime()))
    op.create_table("payroll_payments", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("period_id", sa.Integer(), sa.ForeignKey("payroll_periods.id"), nullable=False), sa.Column("branch_id", sa.Integer(), sa.ForeignKey("branches.id"), nullable=False), sa.Column("amount", sa.Numeric(14, 2), nullable=False), sa.Column("payment_date", sa.Date(), nullable=False), sa.Column("payment_method", sa.String(40), nullable=False, server_default="EFECTIVO"), sa.Column("reference", sa.String(120)), sa.Column("status", sa.String(20), nullable=False, server_default="PAID"), sa.Column("created_by", sa.String(160)), sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()), sa.Column("void_reason", sa.String(240)), sa.Column("voided_by", sa.String(160)), sa.Column("voided_at", sa.DateTime()))


def downgrade() -> None:
    op.drop_table("payroll_payments")
    op.drop_table("payroll_adjustments")
    op.drop_column("payroll_calculations", "additions_pay")
    op.drop_constraint("uq_payroll_branch_period_range", "payroll_periods", type_="unique")
    op.create_unique_constraint("uq_payroll_period_range", "payroll_periods", ["date_from", "date_to"])
    op.drop_constraint("fk_payroll_period_branch", "payroll_periods", type_="foreignkey")
    op.drop_column("payroll_periods", "branch_id")
