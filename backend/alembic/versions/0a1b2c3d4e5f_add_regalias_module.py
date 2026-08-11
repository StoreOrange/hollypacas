"""add regalias module

Revision ID: 0a1b2c3d4e5f
Revises: f7a8b9c0d1e2
Create Date: 2026-08-10 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0a1b2c3d4e5f"
down_revision: Union[str, Sequence[str], None] = "f7a8b9c0d1e2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())

    if "regalias_productos" not in tables:
        op.create_table(
            "regalias_productos",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("producto_id", sa.Integer(), nullable=False),
            sa.Column("nota", sa.String(length=240), nullable=True),
            sa.Column("activo", sa.Boolean(), nullable=True),
            sa.Column("usuario_registro", sa.String(length=120), nullable=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
            sa.ForeignKeyConstraint(["producto_id"], ["productos.id"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("producto_id", name="uq_regalia_producto"),
        )
        op.create_index(op.f("ix_regalias_productos_id"), "regalias_productos", ["id"], unique=False)

    if "regalias_vendedores_politicas" not in tables:
        op.create_table(
            "regalias_vendedores_politicas",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("vendedor_id", sa.Integer(), nullable=False),
            sa.Column("presupuesto_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
            sa.Column("activo", sa.Boolean(), nullable=True),
            sa.Column("usuario_registro", sa.String(length=120), nullable=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
            sa.ForeignKeyConstraint(["vendedor_id"], ["vendedores.id"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("vendedor_id", name="uq_regalia_vendedor_politica"),
        )
        op.create_index(op.f("ix_regalias_vendedores_politicas_id"), "regalias_vendedores_politicas", ["id"], unique=False)

    if "regalias_vendedores_items" not in tables:
        op.create_table(
            "regalias_vendedores_items",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("politica_id", sa.Integer(), nullable=False),
            sa.Column("producto_id", sa.Integer(), nullable=False),
            sa.Column("cantidad_disponible", sa.Numeric(14, 2), nullable=False, server_default="0"),
            sa.Column("activo", sa.Boolean(), nullable=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
            sa.ForeignKeyConstraint(["politica_id"], ["regalias_vendedores_politicas.id"]),
            sa.ForeignKeyConstraint(["producto_id"], ["productos.id"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("politica_id", "producto_id", name="uq_regalia_vendedor_item"),
        )
        op.create_index(op.f("ix_regalias_vendedores_items_id"), "regalias_vendedores_items", ["id"], unique=False)


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "regalias_vendedores_items" in tables:
        op.drop_index(op.f("ix_regalias_vendedores_items_id"), table_name="regalias_vendedores_items")
        op.drop_table("regalias_vendedores_items")
    if "regalias_vendedores_politicas" in tables:
        op.drop_index(op.f("ix_regalias_vendedores_politicas_id"), table_name="regalias_vendedores_politicas")
        op.drop_table("regalias_vendedores_politicas")
    if "regalias_productos" in tables:
        op.drop_index(op.f("ix_regalias_productos_id"), table_name="regalias_productos")
        op.drop_table("regalias_productos")
