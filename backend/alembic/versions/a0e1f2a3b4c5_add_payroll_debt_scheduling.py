"""add payroll debt scheduling and dependencies

Revision ID: a0e1f2a3b4c5
Revises: 9d0e1f2a3b4c
"""
import sqlalchemy as sa
from alembic import op

revision = "a0e1f2a3b4c5"
down_revision = "9d0e1f2a3b4c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("payroll_employee_deductions")}
    additions = {
        "depends_on_id": sa.Column("depends_on_id", sa.Integer(), nullable=True),
        "paused_until": sa.Column("paused_until", sa.Date(), nullable=True),
        "pause_reason": sa.Column("pause_reason", sa.String(240), nullable=True),
        "paused_at": sa.Column("paused_at", sa.DateTime(), nullable=True),
        "updated_by": sa.Column("updated_by", sa.String(160), nullable=True),
        "updated_at": sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    }
    for name, column in additions.items():
        if name not in columns:
            op.add_column("payroll_employee_deductions", column)
    foreign_keys = inspector.get_foreign_keys("payroll_employee_deductions")
    if not any(key.get("constrained_columns") == ["depends_on_id"] for key in foreign_keys):
        op.create_foreign_key("fk_payroll_deduction_dependency", "payroll_employee_deductions", "payroll_employee_deductions", ["depends_on_id"], ["id"])


def downgrade() -> None:
    op.drop_constraint("fk_payroll_deduction_dependency", "payroll_employee_deductions", type_="foreignkey")
    for column in ("updated_at", "updated_by", "paused_at", "pause_reason", "paused_until", "depends_on_id"):
        op.drop_column("payroll_employee_deductions", column)
