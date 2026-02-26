"""add tipo_mov to cobranza_abonos

Revision ID: f7a8b9c0d1e2
Revises: e6f7a8b9c0d1
Create Date: 2026-02-26 12:05:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f7a8b9c0d1e2"
down_revision: Union[str, Sequence[str], None] = "e6f7a8b9c0d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "cobranza_abonos",
        sa.Column("tipo_mov", sa.String(length=20), nullable=False, server_default="ABONO"),
    )


def downgrade() -> None:
    op.drop_column("cobranza_abonos", "tipo_mov")

