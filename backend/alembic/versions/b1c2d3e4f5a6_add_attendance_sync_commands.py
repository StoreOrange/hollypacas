"""add attendance sync commands

Revision ID: b1c2d3e4f5a6
Revises: a0e1f2a3b4c5
"""

import sqlalchemy as sa
from alembic import op

revision = "b1c2d3e4f5a6"
down_revision = "a0e1f2a3b4c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "attendance_sync_commands",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("device_code", sa.String(50), nullable=False),
        sa.Column("requested_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime()),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
    )
    op.create_index("ix_attendance_sync_commands_device_code", "attendance_sync_commands", ["device_code"])
    op.create_index("ix_attendance_sync_commands_status", "attendance_sync_commands", ["status"])


def downgrade() -> None:
    op.drop_index("ix_attendance_sync_commands_status", table_name="attendance_sync_commands")
    op.drop_index("ix_attendance_sync_commands_device_code", table_name="attendance_sync_commands")
    op.drop_table("attendance_sync_commands")
