"""Choose commissions from the promotion recorded on the sold line."""
from decimal import Decimal, InvalidOperation


def is_special_price_sale(item, *, enabled=True):
    return bool(enabled and item is not None
                and str(getattr(item, "combo_role", "") or "").strip().lower() == "discount"
                and str(getattr(item, "combo_group", "") or "").startswith("discount2-"))


def commission_rate(config, item, *, enabled=True):
    if is_special_price_sale(item, enabled=enabled):
        value = getattr(config, "comision_promocion_usd", None)
        return Decimal(str(value)) if value is not None else None
    return Decimal(str(getattr(config, "comision_usd", 0) or 0))


def parse_commission_amount(raw, *, optional=False):
    value = str(raw or "").strip()
    if not value:
        return None if optional else Decimal("0.00")
    try:
        amount = Decimal(value.replace(",", ""))
        if not amount.is_finite() or amount < 0 or amount >= Decimal("1000000000000"):
            raise ValueError
        rounded = amount.quantize(Decimal("0.01"))
        if amount != rounded:
            raise ValueError
        return rounded
    except (InvalidOperation, ValueError):
        raise ValueError("La comision debe ser un importe no negativo con hasta dos decimales.")
