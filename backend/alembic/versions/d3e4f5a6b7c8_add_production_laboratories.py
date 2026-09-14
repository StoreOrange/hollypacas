"""add production laboratories

Revision ID: d3e4f5a6b7c8
Revises: c2d3e4f5a6b7
"""

import sqlalchemy as sa
from alembic import op

revision = "d3e4f5a6b7c8"
down_revision = "c2d3e4f5a6b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "production_laboratories",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("numero", sa.String(40), nullable=False),
        sa.Column("fecha", sa.Date(), nullable=False),
        sa.Column("estado", sa.String(24), nullable=False, server_default="TERMINADA"),
        sa.Column("bodega_origen_id", sa.Integer(), sa.ForeignKey("bodegas.id"), nullable=False),
        sa.Column("bodega_destino_id", sa.Integer(), sa.ForeignKey("bodegas.id"), nullable=False),
        sa.Column("observacion", sa.String(500), nullable=True),
        sa.Column("usuario_registro", sa.String(120), nullable=True),
        sa.Column("terminada_at", sa.DateTime(), nullable=True),
        sa.Column("cerrada_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("numero"),
    )
    op.create_index("ix_production_laboratories_numero", "production_laboratories", ["numero"])
    op.create_index("ix_production_laboratories_fecha", "production_laboratories", ["fecha"])
    op.create_index("ix_production_laboratories_estado", "production_laboratories", ["estado"])
    op.create_table(
        "production_laboratory_movements",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("laboratorio_id", sa.Integer(), sa.ForeignKey("production_laboratories.id"), nullable=False),
        sa.Column("egreso_id", sa.Integer(), sa.ForeignKey("egresos_inventario.id"), nullable=True, unique=True),
        sa.Column("ingreso_id", sa.Integer(), sa.ForeignKey("ingresos_inventario.id"), nullable=True, unique=True),
        sa.Column("clase", sa.String(20), nullable=False, server_default="INICIAL"),
        sa.Column("observacion", sa.String(300), nullable=True),
        sa.Column("usuario_registro", sa.String(120), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_production_laboratory_movements_laboratorio_id", "production_laboratory_movements", ["laboratorio_id"])


def downgrade() -> None:
    op.drop_table("production_laboratory_movements")
    op.drop_table("production_laboratories")
