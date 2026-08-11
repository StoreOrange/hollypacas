"""add venta item promo policy

Revision ID: 1b2c3d4e5f6a
Revises: 0a1b2c3d4e5f
Create Date: 2026-08-10
"""

from alembic import op
import sqlalchemy as sa


revision = "1b2c3d4e5f6a"
down_revision = "0a1b2c3d4e5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "ventas_items" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ventas_items")}
        if "promo_policy" not in columns:
            op.add_column("ventas_items", sa.Column("promo_policy", sa.String(length=20), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "ventas_items" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ventas_items")}
        if "promo_policy" in columns:
            op.drop_column("ventas_items", "promo_policy")
