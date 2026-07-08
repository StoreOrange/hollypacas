"""add stagnant inventory catalog

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-07-08 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "productos_estancados" in inspector.get_table_names():
        return
    op.create_table(
        "productos_estancados",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("producto_id", sa.Integer(), nullable=False),
        sa.Column("motivo", sa.String(length=240), nullable=True),
        sa.Column("activo", sa.Boolean(), nullable=True),
        sa.Column("usuario_registro", sa.String(length=120), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["producto_id"], ["productos.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("producto_id", name="uq_producto_estancado_producto"),
    )
    op.create_index(op.f("ix_productos_estancados_id"), "productos_estancados", ["id"], unique=False)


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "productos_estancados" not in inspector.get_table_names():
        return
    op.drop_index(op.f("ix_productos_estancados_id"), table_name="productos_estancados")
    op.drop_table("productos_estancados")
