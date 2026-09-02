"""add attendance and HR module

Revision ID: 3d4e5f6a7b8c
Revises: 2c3d4e5f6a7b, c3d4e5f6a7b8
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "3d4e5f6a7b8c"
down_revision: Union[str, Sequence[str], None] = ("2c3d4e5f6a7b", "c3d4e5f6a7b8")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "hr_areas",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(40), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("code"),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_hr_areas_code", "hr_areas", ["code"])
    op.create_table(
        "hr_positions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(40), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("code"),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_hr_positions_code", "hr_positions", ["code"])
    op.create_table(
        "hr_employees",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("employee_code", sa.String(40), nullable=False),
        sa.Column("full_name", sa.String(160), nullable=False),
        sa.Column("identification", sa.String(40)),
        sa.Column("email", sa.String(160)),
        sa.Column("phone", sa.String(40)),
        sa.Column("hire_date", sa.Date()),
        sa.Column("termination_date", sa.Date()),
        sa.Column("status", sa.String(20), nullable=False, server_default="ACTIVE"),
        sa.Column("area_id", sa.Integer(), sa.ForeignKey("hr_areas.id")),
        sa.Column("position_id", sa.Integer(), sa.ForeignKey("hr_positions.id")),
        sa.Column("branch_id", sa.Integer(), sa.ForeignKey("branches.id")),
        sa.Column("payroll_user_id", sa.Integer(), sa.ForeignKey("users.id")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("employee_code"),
        sa.UniqueConstraint("identification"),
    )
    op.create_index("ix_hr_employees_employee_code", "hr_employees", ["employee_code"])
    op.create_index("ix_hr_employees_full_name", "hr_employees", ["full_name"])
    op.create_table(
        "attendance_devices",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(50), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("model", sa.String(80)),
        sa.Column("serial_number", sa.String(100)),
        sa.Column("local_ip", sa.String(64)),
        sa.Column("local_port", sa.Integer(), nullable=False, server_default="4370"),
        sa.Column("branch_id", sa.Integer(), sa.ForeignKey("branches.id")),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("last_seen_at", sa.DateTime()),
        sa.Column("last_sync_at", sa.DateTime()),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("code"),
    )
    op.create_index("ix_attendance_devices_code", "attendance_devices", ["code"])
    op.create_table(
        "attendance_device_users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("device_id", sa.Integer(), sa.ForeignKey("attendance_devices.id", ondelete="CASCADE"), nullable=False),
        sa.Column("device_user_id", sa.String(40), nullable=False),
        sa.Column("device_uid", sa.Integer()),
        sa.Column("device_name", sa.String(160)),
        sa.Column("card_number", sa.String(80)),
        sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id")),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("first_seen_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("device_id", "device_user_id", name="uq_attendance_device_user"),
    )
    op.create_table(
        "attendance_punches",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_event_key", sa.String(160), nullable=False),
        sa.Column("device_id", sa.Integer(), sa.ForeignKey("attendance_devices.id"), nullable=False),
        sa.Column("device_user_id", sa.String(40), nullable=False),
        sa.Column("employee_id", sa.Integer(), sa.ForeignKey("hr_employees.id")),
        sa.Column("occurred_at", sa.DateTime(), nullable=False),
        sa.Column("punch_state", sa.Integer()),
        sa.Column("verify_mode", sa.Integer()),
        sa.Column("work_code", sa.String(40)),
        sa.Column("raw_payload", sa.Text()),
        sa.Column("received_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("source_event_key", name="uq_attendance_source_event_key"),
    )
    op.create_index("ix_attendance_source_event_key", "attendance_punches", ["source_event_key"])
    op.create_index("ix_attendance_punch_user", "attendance_punches", ["device_user_id"])
    op.create_index("ix_attendance_punch_employee", "attendance_punches", ["employee_id"])
    op.create_index("ix_attendance_punch_occurred", "attendance_punches", ["occurred_at"])
    op.create_table(
        "attendance_sync_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("device_id", sa.Integer(), sa.ForeignKey("attendance_devices.id")),
        sa.Column("started_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime()),
        sa.Column("status", sa.String(20), nullable=False, server_default="RUNNING"),
        sa.Column("received_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("inserted_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text()),
    )
    op.create_table(
        "attendance_policy_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("weekday_overtime_start", sa.Time(), nullable=False, server_default="17:00:00"),
        sa.Column("saturday_overtime_start", sa.Time(), nullable=False, server_default="16:00:00"),
        sa.Column("sunday_all_day_overtime", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("expected_daily_minutes", sa.Integer(), nullable=False, server_default="480"),
        sa.Column("break_minutes", sa.Integer(), nullable=False, server_default="60"),
        sa.Column("break_after_minutes", sa.Integer(), nullable=False, server_default="360"),
        sa.Column("updated_by", sa.String(160)),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("attendance_policy_settings")
    op.drop_table("attendance_sync_runs")
    op.drop_index("ix_attendance_punch_occurred", table_name="attendance_punches")
    op.drop_index("ix_attendance_punch_employee", table_name="attendance_punches")
    op.drop_index("ix_attendance_punch_user", table_name="attendance_punches")
    op.drop_index("ix_attendance_source_event_key", table_name="attendance_punches")
    op.drop_table("attendance_punches")
    op.drop_table("attendance_device_users")
    op.drop_index("ix_attendance_devices_code", table_name="attendance_devices")
    op.drop_table("attendance_devices")
    op.drop_index("ix_hr_employees_full_name", table_name="hr_employees")
    op.drop_index("ix_hr_employees_employee_code", table_name="hr_employees")
    op.drop_table("hr_employees")
    op.drop_index("ix_hr_positions_code", table_name="hr_positions")
    op.drop_table("hr_positions")
    op.drop_index("ix_hr_areas_code", table_name="hr_areas")
    op.drop_table("hr_areas")
