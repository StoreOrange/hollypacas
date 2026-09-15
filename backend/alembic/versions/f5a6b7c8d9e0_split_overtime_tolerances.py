"""split weekday and saturday overtime tolerances

Revision ID: f5a6b7c8d9e0
Revises: e4f5a6b7c8d9
"""

import sqlalchemy as sa
from alembic import op

revision = "f5a6b7c8d9e0"
down_revision = "e4f5a6b7c8d9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    columns = {column["name"] for column in inspector.get_columns("attendance_policy_settings")}
    if "weekday_overtime_grace_minutes" not in columns:
        op.add_column(
            "attendance_policy_settings",
            sa.Column("weekday_overtime_grace_minutes", sa.Integer(), nullable=False, server_default="15"),
        )
    if "saturday_overtime_grace_minutes" not in columns:
        op.add_column(
            "attendance_policy_settings",
            sa.Column("saturday_overtime_grace_minutes", sa.Integer(), nullable=False, server_default="15"),
        )
    # Conserva configuraciones existentes cuando ya se había personalizado el
    # margen único de horas extra.
    connection.execute(
        sa.text(
            "UPDATE attendance_policy_settings "
            "SET weekday_overtime_grace_minutes = overtime_grace_minutes, "
            "saturday_overtime_grace_minutes = overtime_grace_minutes"
        )
    )


def downgrade() -> None:
    op.drop_column("attendance_policy_settings", "saturday_overtime_grace_minutes")
    op.drop_column("attendance_policy_settings", "weekday_overtime_grace_minutes")
