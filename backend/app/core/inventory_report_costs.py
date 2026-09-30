"""Valuation adjustments exclusive to Pacas Hollywood's consolidated report."""
from decimal import Decimal, ROUND_HALF_UP

CONSOLIDATED_COST_INCREASES = {
    'HL31': Decimal('3'),
    'DL17': Decimal('3'),
    'DL102': Decimal('1'),
    'USM84': Decimal('4'),
    'USM80': Decimal('3'),
}


# Calibrated on Central's existing increase: add C$36,450 through percentages.
# Fixed multiplier: subsequent changes in stock or cost change the increase.
CENTRAL_INCREASE_FACTOR = Decimal('137138.10') / Decimal('100688.10')


def consolidated_cost(code: str, unit_cost: Decimal, quantity: Decimal, *, enabled: bool):
    percentage = (CONSOLIDATED_COST_INCREASES.get((code or '').strip().upper(), Decimal('0'))
                  if enabled and quantity > 0 else Decimal('0'))
    total = unit_cost * quantity * (Decimal('1') + percentage / Decimal('100'))
    return total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP), percentage


def branch_cost_increases(branch_code: str, items: list[dict], *, enabled: bool) -> dict:
    """Allocate report-only increases per branch; Esteli shares an exact C$30,356.

    Items use unique product IDs. Only positive stock with positive cost qualifies.
    Esteli keeps the relative weights of the configured product percentages and
    assigns rounding remainders deterministically in whole cordobas.
    """
    if not enabled:
        return {}
    code = (branch_code or '').strip().lower()
    if code not in {'central', 'esteli'}:
        return {}
    weights = {}
    for item in items:
        pct = CONSOLIDATED_COST_INCREASES.get((item['codigo'] or '').strip().upper(), Decimal('0'))
        if item['cantidad'] > 0 and item['costo_unitario'] > 0 and pct:
            weights[item['id']] = item['cantidad'] * item['costo_unitario'] * pct / Decimal('100')
    total_weight = sum(weights.values(), Decimal('0'))
    if not total_weight:
        return {}
    if code == 'central':
        shares = {key: weight * CENTRAL_INCREASE_FACTOR for key, weight in weights.items()}
        unit = Decimal('.01')
        target = (total_weight * CENTRAL_INCREASE_FACTOR).quantize(unit, rounding=ROUND_HALF_UP)
    else:
        target = Decimal('30356')
        shares = {key: target * weight / total_weight for key, weight in weights.items()}
        unit = Decimal('1')
    allocated = {key: Decimal(int(share / unit)) * unit for key, share in shares.items()}
    remainder = int((target - sum(allocated.values())) / unit)
    ordered = sorted(shares, key=lambda key: (-(shares[key] - allocated[key]), key))
    for key in ordered[:remainder]:
        allocated[key] += unit
    return allocated
