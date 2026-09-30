"""Validation and optimistic revisions for commission assignment edits."""
from decimal import Decimal, InvalidOperation
import hashlib
import json


def whole_quantity(value):
    if isinstance(value, bool):
        raise ValueError("La cantidad debe ser un entero no negativo.")
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0 or amount != amount.to_integral_value():
            raise ValueError
        if amount > 999999999999:
            raise ValueError
        return int(amount)
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("La cantidad debe ser un entero no negativo, sin decimales.")


def assignment_revision(rows):
    state = sorted((int(r.id), int(r.vendedor_asignado_id), str(Decimal(str(r.cantidad)).normalize()),
                    str(r.updated_at or "")) for r in rows)
    return hashlib.sha256(json.dumps(state).encode()).hexdigest()
