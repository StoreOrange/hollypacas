"""scope payroll holidays by branch

Revision ID: b7c8d9e0f1a2
Revises: a6b7c8d9e0f1
"""

import sqlalchemy as sa
from alembic import op

revision = "b7c8d9e0f1a2"
down_revision = "a6b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    columns = {column["name"] for column in inspector.get_columns("payroll_holidays")}
    if "branch_id" not in columns:
        op.add_column("payroll_holidays", sa.Column("branch_id", sa.Integer(), nullable=True))
        op.create_foreign_key("fk_payroll_holidays_branch_id", "payroll_holidays", "branches", ["branch_id"], ["id"])
    connection.execute(sa.text("UPDATE payroll_holidays h SET branch_id = p.branch_id FROM payroll_periods p WHERE h.period_id = p.id AND h.branch_id IS NULL"))
    inspector = sa.inspect(connection)
    for constraint in inspector.get_unique_constraints("payroll_holidays"):
        if constraint.get("column_names") == ["holiday_date"] and constraint.get("name"):
            op.drop_constraint(constraint["name"], "payroll_holidays", type_="unique")
    constraints = {constraint.get("name") for constraint in sa.inspect(connection).get_unique_constraints("payroll_holidays")}
    if "uq_payroll_holiday_branch_date" not in constraints:
        op.create_unique_constraint("uq_payroll_holiday_branch_date", "payroll_holidays", ["branch_id", "holiday_date"])


def downgrade() -> None:
    op.drop_constraint("uq_payroll_holiday_branch_date", "payroll_holidays", type_="unique")
    # Registros repetidos entre sucursales deben resolverse antes de volver a
    # imponer la unicidad global; por seguridad no se destruyen datos aquí.
    op.drop_constraint("fk_payroll_holidays_branch_id", "payroll_holidays", type_="foreignkey")
    op.drop_column("payroll_holidays", "branch_id")
