"""add accounting subrubro rules table

Revision ID: d4e5f6a7b8c9
Revises: c1d2e3f4a5b6
Create Date: 2026-02-24 11:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, Sequence[str], None] = "c1d2e3f4a5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "accounting_subrubro_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=60), nullable=False),
        sa.Column("label", sa.String(length=160), nullable=False),
        sa.Column("intent_code", sa.String(length=20), nullable=False, server_default="EGRESO"),
        sa.Column("debit_terms", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("credit_terms", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("credit_cash_terms", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("credit_credit_terms", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("default_concept", sa.String(length=220), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="1000"),
        sa.Column("activo", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("updated_by", sa.String(length=160), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code", name="uq_accounting_subrubro_rules_code"),
    )
    op.create_index(
        op.f("ix_accounting_subrubro_rules_id"),
        "accounting_subrubro_rules",
        ["id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_accounting_subrubro_rules_id"), table_name="accounting_subrubro_rules")
    op.drop_table("accounting_subrubro_rules")
