"""add commissions final table

Revision ID: c1d2e3f4a5b6
Revises: 8b9c7a6d5e4f
Create Date: 2026-02-06 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c1d2e3f4a5b6"
down_revision: Union[str, Sequence[str], None] = "8b9c7a6d5e4f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ventas_comisiones_finales",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("fecha", sa.Date(), nullable=False),
        sa.Column("branch_id", sa.Integer(), nullable=True),
        sa.Column("bodega_id", sa.Integer(), nullable=True),
        sa.Column("factura_id", sa.Integer(), nullable=False),
        sa.Column("venta_item_id", sa.Integer(), nullable=False),
        sa.Column("cliente_id", sa.Integer(), nullable=True),
        sa.Column("producto_id", sa.Integer(), nullable=False),
        sa.Column("vendedor_origen_id", sa.Integer(), nullable=True),
        sa.Column("vendedor_asignado_id", sa.Integer(), nullable=False),
        sa.Column("cantidad", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("precio_unitario_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("precio_unitario_cs", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("subtotal_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("subtotal_cs", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("comision_unit_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("comision_total_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("usuario_registro", sa.String(length=120), nullable=True),
        sa.Column("finalizado_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["bodega_id"], ["bodegas.id"]),
        sa.ForeignKeyConstraint(["branch_id"], ["branches.id"]),
        sa.ForeignKeyConstraint(["cliente_id"], ["clientes.id"]),
        sa.ForeignKeyConstraint(["factura_id"], ["ventas_facturas.id"]),
        sa.ForeignKeyConstraint(["producto_id"], ["productos.id"]),
        sa.ForeignKeyConstraint(["vendedor_asignado_id"], ["vendedores.id"]),
        sa.ForeignKeyConstraint(["vendedor_origen_id"], ["vendedores.id"]),
        sa.ForeignKeyConstraint(["venta_item_id"], ["ventas_items.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_ventas_comisiones_finales_id"),
        "ventas_comisiones_finales",
        ["id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_ventas_comisiones_finales_id"), table_name="ventas_comisiones_finales")
    op.drop_table("ventas_comisiones_finales")

