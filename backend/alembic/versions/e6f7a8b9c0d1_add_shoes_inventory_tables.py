"""add shoes inventory tables

Revision ID: e6f7a8b9c0d1
Revises: d4e5f6a7b8c9
Create Date: 2026-02-24 15:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e6f7a8b9c0d1"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "color_catalog",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("nombre", sa.String(length=120), nullable=False),
        sa.Column("abreviatura", sa.String(length=20), nullable=False),
        sa.Column("activo", sa.Boolean(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("nombre"),
    )
    op.create_index(op.f("ix_color_catalog_id"), "color_catalog", ["id"], unique=False)

    op.create_table(
        "shoe_size_formats",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("codigo", sa.String(length=40), nullable=False),
        sa.Column("nombre", sa.String(length=160), nullable=False),
        sa.Column("activo", sa.Boolean(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("codigo"),
    )
    op.create_index(op.f("ix_shoe_size_formats_id"), "shoe_size_formats", ["id"], unique=False)

    op.create_table(
        "shoe_size_format_lines",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("formato_id", sa.Integer(), nullable=False),
        sa.Column("talla", sa.String(length=20), nullable=False),
        sa.Column("cantidad", sa.Integer(), nullable=False),
        sa.Column("orden", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["formato_id"], ["shoe_size_formats.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_shoe_size_format_lines_id"), "shoe_size_format_lines", ["id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_shoe_size_format_lines_id"), table_name="shoe_size_format_lines")
    op.drop_table("shoe_size_format_lines")
    op.drop_index(op.f("ix_shoe_size_formats_id"), table_name="shoe_size_formats")
    op.drop_table("shoe_size_formats")
    op.drop_index(op.f("ix_color_catalog_id"), table_name="color_catalog")
    op.drop_table("color_catalog")
