"""add manual early overtime authorization

Revision ID: a6b7c8d9e0f1
Revises: f5a6b7c8d9e0
"""

import sqlalchemy as sa
from alembic import op

revision = "a6b7c8d9e0f1"
down_revision = "f5a6b7c8d9e0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("attendance_day_overrides")}
    if "authorize_early_overtime" not in columns:
        op.add_column("attendance_day_overrides", sa.Column("authorize_early_overtime", sa.Boolean(), nullable=False, server_default=sa.false()))
    if "early_overtime_note" not in columns:
        op.add_column("attendance_day_overrides", sa.Column("early_overtime_note", sa.String(length=240), nullable=True))


def downgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("attendance_day_overrides")}
    if "early_overtime_note" in columns:
        op.drop_column("attendance_day_overrides", "early_overtime_note")
    if "authorize_early_overtime" in columns:
        op.drop_column("attendance_day_overrides", "authorize_early_overtime")
