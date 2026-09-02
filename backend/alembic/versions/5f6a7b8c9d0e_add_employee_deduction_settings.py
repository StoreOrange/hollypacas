"""add per-employee deduction applicability

Revision ID: 5f6a7b8c9d0e
Revises: 4e5f6a7b8c9d
"""

import sqlalchemy as sa
from alembic import op

revision = "5f6a7b8c9d0e"
down_revision = "4e5f6a7b8c9d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payroll_employee_deduction_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id", ondelete="CASCADE"), nullable=False),
        sa.Column("deduction_type_id", sa.Integer(), sa.ForeignKey("payroll_deduction_types.id", ondelete="CASCADE"), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("employee_id", "deduction_type_id", name="uq_payroll_employee_deduction_setting"),
    )


def downgrade() -> None:
    op.drop_table("payroll_employee_deduction_settings")
