"""add regalia vendor unit quota

Revision ID: 2c3d4e5f6a7b
Revises: 1b2c3d4e5f6a
Create Date: 2026-08-11
"""

from alembic import op
import sqlalchemy as sa


revision = "2c3d4e5f6a7b"
down_revision = "1b2c3d4e5f6a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "regalias_vendedores_politicas" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("regalias_vendedores_politicas")}
        if "cupo_unidades_total" not in columns:
            op.add_column(
                "regalias_vendedores_politicas",
                sa.Column("cupo_unidades_total", sa.Numeric(14, 2), nullable=False, server_default="0"),
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "regalias_vendedores_politicas" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("regalias_vendedores_politicas")}
        if "cupo_unidades_total" in columns:
            op.drop_column("regalias_vendedores_politicas", "cupo_unidades_total")
