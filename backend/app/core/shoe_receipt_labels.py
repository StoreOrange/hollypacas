"""Receipt quantities are the only source of truth for receipt label jobs."""
from decimal import Decimal, InvalidOperation


def units(value):
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError('Las cantidades de zapatos deben ser números enteros.') from exc
    if not quantity.is_finite() or quantity < 0 or quantity != quantity.to_integral_value():
        raise ValueError('Las cantidades de zapatos deben ser enteras y no negativas; no se redondean etiquetas.')
    return int(quantity)


def receipt_rows(receipt, scan_code):
    grouped = {}
    for item in receipt.items:
        quantity = units(item.cantidad or 0)
        if not quantity:
            continue
        variant = item.variante
        product = item.producto
        if not variant or not product:
            raise ValueError('Este ingreso contiene unidades sin variante de zapatos. Revise el ingreso antes de imprimir.')
        key = int(variant.id)
        if key not in grouped:
            grouped[key] = {
                'variant_id': key, 'product_id': int(product.id),
                'model': product.cod_producto, 'name': product.descripcion,
                'code': variant.cod_variante, 'scan_code': scan_code(key),
                'color': variant.color.nombre if variant.color else '', 'size': variant.talla,
                'quantity': 0, 'price': float(product.precio_venta1 or 0),
            }
        grouped[key]['quantity'] += quantity
    return sorted(grouped.values(), key=lambda r:(r['model'], r['product_id'], r['color'], str(r['size'])))


def selected_rows(rows, ids):
    if not isinstance(ids,list) or not ids or any(type(v) is not int or v < 1 for v in ids):
        raise ValueError('Seleccione al menos una variante del ingreso.')
    if len(ids) != len(set(ids)):
        raise ValueError('La selección contiene variantes repetidas.')
    available = {r['variant_id']:r for r in rows}
    if not set(ids).issubset(available):
        raise ValueError('La selección contiene variantes ajenas a este ingreso.')
    result = [r for r in rows if r['variant_id'] in set(ids)]
    if sum(r['quantity'] for r in result)>5000:
        raise ValueError('Seleccione menos variantes: máximo 5,000 etiquetas por envío. Las cantidades no se recortan.')
    return result


def print_rows(rows):
    return [{'name':r['name'], 'detail':f"{r['color']} | Talla {r['size']}",
             'code':r['scan_code'], 'reference':f"Ref. {r['model']}",
             'price':f"C$ {r['price']:,.2f}", 'quantity':r['quantity']} for r in rows]
