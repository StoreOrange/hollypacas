"""add per-period payroll deduction overrides

Revision ID: 8c9d0e1f2a3b
Revises: 7b8c9d0e1f2a
"""
import sqlalchemy as sa
from alembic import op

revision = "8c9d0e1f2a3b"
down_revision = "7b8c9d0e1f2a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payroll_deduction_overrides",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("period_id", sa.Integer(), sa.ForeignKey("payroll_periods.id", ondelete="CASCADE"), nullable=False),
        sa.Column("employee_deduction_id", sa.Integer(), sa.ForeignKey("payroll_employee_deductions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("apply_charge", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("override_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("reason", sa.String(240), nullable=False),
        sa.Column("created_by", sa.String(160), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("period_id", "employee_deduction_id", name="uq_payroll_period_deduction_override"),
    )


def downgrade() -> None:
    op.drop_table("payroll_deduction_overrides")
