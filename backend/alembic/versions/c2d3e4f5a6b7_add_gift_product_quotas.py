"""add gift product quotas

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
"""

import sqlalchemy as sa
from alembic import op

revision = "c2d3e4f5a6b7"
down_revision = "b1c2d3e4f5a6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("regalias_productos", sa.Column("cantidad_total", sa.Numeric(14, 2), nullable=False, server_default="0"))
    op.add_column("regalias_productos", sa.Column("modo_asignacion", sa.String(20), nullable=False, server_default="LIBRE"))


def downgrade() -> None:
    op.drop_column("regalias_productos", "modo_asignacion")
    op.drop_column("regalias_productos", "cantidad_total")
