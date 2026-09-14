"""add attendance tolerance policy and daily overrides

Revision ID: e4f5a6b7c8d9
Revises: d3e4f5a6b7c8
"""

import sqlalchemy as sa
from alembic import op

revision = "e4f5a6b7c8d9"
down_revision = "d3e4f5a6b7c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    policy_columns = {column["name"] for column in inspector.get_columns("attendance_policy_settings")}
    if "weekday_start" not in policy_columns:
        op.add_column("attendance_policy_settings", sa.Column("weekday_start", sa.Time(), nullable=False, server_default="08:00:00"))
    if "entry_grace_minutes" not in policy_columns:
        op.add_column("attendance_policy_settings", sa.Column("entry_grace_minutes", sa.Integer(), nullable=False, server_default="20"))
    if "overtime_grace_minutes" not in policy_columns:
        op.add_column("attendance_policy_settings", sa.Column("overtime_grace_minutes", sa.Integer(), nullable=False, server_default="15"))
    calculation_columns = {column["name"] for column in inspector.get_columns("payroll_calculations")}
    if "late_minutes" not in calculation_columns:
        op.add_column("payroll_calculations", sa.Column("late_minutes", sa.Integer(), nullable=False, server_default="0"))
    if "late_deduction" not in calculation_columns:
        op.add_column("payroll_calculations", sa.Column("late_deduction", sa.Numeric(14, 2), nullable=False, server_default="0"))
    if "attendance_day_overrides" in inspector.get_table_names():
        return
    op.create_table(
        "attendance_day_overrides",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id", ondelete="CASCADE"), nullable=False),
        sa.Column("work_date", sa.Date(), nullable=False),
        sa.Column("exclude_overtime", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("waive_lateness", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("note", sa.String(240), nullable=True),
        sa.Column("updated_by", sa.String(160), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("employee_id", "work_date", name="uq_attendance_day_override_employee_date"),
    )
    op.create_index("ix_attendance_day_overrides_employee_id", "attendance_day_overrides", ["employee_id"])
    op.create_index("ix_attendance_day_overrides_work_date", "attendance_day_overrides", ["work_date"])


def downgrade() -> None:
    op.drop_table("attendance_day_overrides")
    op.drop_column("payroll_calculations", "late_deduction")
    op.drop_column("payroll_calculations", "late_minutes")
    op.drop_column("attendance_policy_settings", "overtime_grace_minutes")
    op.drop_column("attendance_policy_settings", "entry_grace_minutes")
    op.drop_column("attendance_policy_settings", "weekday_start")
