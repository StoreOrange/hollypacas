"""add tipo to recibos rubros

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-07-09 10:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "recibos_rubros" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("recibos_rubros")}
    if "tipo" not in columns:
        op.add_column("recibos_rubros", sa.Column("tipo", sa.String(length=20), nullable=False, server_default="AMBOS"))
        op.execute("UPDATE recibos_rubros SET tipo = 'AMBOS' WHERE tipo IS NULL OR tipo = ''")


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "recibos_rubros" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("recibos_rubros")}
    if "tipo" in columns:
        op.drop_column("recibos_rubros", "tipo")
