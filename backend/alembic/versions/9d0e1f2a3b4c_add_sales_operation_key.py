"""add idempotency key to sales invoices

Revision ID: 9d0e1f2a3b4c
Revises: 8c9d0e1f2a3b
"""
import sqlalchemy as sa
from alembic import op

revision = "9d0e1f2a3b4c"
down_revision = "8c9d0e1f2a3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    columns = {column["name"] for column in inspector.get_columns("ventas_facturas")}
    if "operation_key" not in columns:
        op.add_column("ventas_facturas", sa.Column("operation_key", sa.String(length=32), nullable=True))
    indexes = {index["name"] for index in inspector.get_indexes("ventas_facturas")}
    if "ix_ventas_facturas_operation_key" not in indexes:
        op.create_index("ix_ventas_facturas_operation_key", "ventas_facturas", ["operation_key"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_ventas_facturas_operation_key", table_name="ventas_facturas")
    op.drop_column("ventas_facturas", "operation_key")
