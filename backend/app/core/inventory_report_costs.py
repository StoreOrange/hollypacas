"""Valuation adjustments exclusive to Pacas Hollywood's consolidated report."""
from decimal import Decimal, ROUND_HALF_UP

CONSOLIDATED_COST_INCREASES = {
    'HL31': Decimal('3'),
    'DL17': Decimal('3'),
    'DL102': Decimal('1'),
    'USM84': Decimal('4'),
    'USM80': Decimal('3'),
}


def consolidated_cost(code: str, unit_cost: Decimal, quantity: Decimal, *, enabled: bool):
    percentage = (CONSOLIDATED_COST_INCREASES.get((code or '').strip().upper(), Decimal('0'))
                  if enabled and quantity > 0 else Decimal('0'))
    total = unit_cost * quantity * (Decimal('1') + percentage / Decimal('100'))
    return total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP), percentage
