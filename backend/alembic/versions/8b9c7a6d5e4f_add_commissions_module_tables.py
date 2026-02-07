"""add commissions module tables

Revision ID: 8b9c7a6d5e4f
Revises: 5285f6b056a5
Create Date: 2026-02-06 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8b9c7a6d5e4f"
down_revision: Union[str, Sequence[str], None] = "5285f6b056a5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "productos_comisiones",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("producto_id", sa.Integer(), nullable=False),
        sa.Column("comision_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("usuario_registro", sa.String(length=120), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["producto_id"], ["productos.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("producto_id", name="uq_producto_comision_producto"),
    )
    op.create_index(op.f("ix_productos_comisiones_id"), "productos_comisiones", ["id"], unique=False)

    op.create_table(
        "ventas_comisiones_asignaciones",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("venta_item_id", sa.Integer(), nullable=False),
        sa.Column("factura_id", sa.Integer(), nullable=False),
        sa.Column("branch_id", sa.Integer(), nullable=True),
        sa.Column("bodega_id", sa.Integer(), nullable=True),
        sa.Column("cliente_id", sa.Integer(), nullable=True),
        sa.Column("producto_id", sa.Integer(), nullable=False),
        sa.Column("fecha", sa.Date(), nullable=False),
        sa.Column("vendedor_origen_id", sa.Integer(), nullable=True),
        sa.Column("vendedor_asignado_id", sa.Integer(), nullable=False),
        sa.Column("cantidad", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("precio_unitario_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("precio_unitario_cs", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("subtotal_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("subtotal_cs", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("usuario_registro", sa.String(length=120), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["venta_item_id"], ["ventas_items.id"]),
        sa.ForeignKeyConstraint(["factura_id"], ["ventas_facturas.id"]),
        sa.ForeignKeyConstraint(["branch_id"], ["branches.id"]),
        sa.ForeignKeyConstraint(["bodega_id"], ["bodegas.id"]),
        sa.ForeignKeyConstraint(["cliente_id"], ["clientes.id"]),
        sa.ForeignKeyConstraint(["producto_id"], ["productos.id"]),
        sa.ForeignKeyConstraint(["vendedor_origen_id"], ["vendedores.id"]),
        sa.ForeignKeyConstraint(["vendedor_asignado_id"], ["vendedores.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_ventas_comisiones_asignaciones_id"),
        "ventas_comisiones_asignaciones",
        ["id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_ventas_comisiones_asignaciones_id"), table_name="ventas_comisiones_asignaciones")
    op.drop_table("ventas_comisiones_asignaciones")
    op.drop_index(op.f("ix_productos_comisiones_id"), table_name="productos_comisiones")
    op.drop_table("productos_comisiones")
