"""Transactional return rules shared by the module and checkout."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from sqlalchemy import func
from ..models.returns import CustomerReturn
from ..models.sales import Cliente, FormaPago, ReciboCaja, ReciboMotivo, ReciboRubro, VentaFactura, VentaItem
from ..models.inventory import Bodega, IngresoInventario, IngresoItem, IngresoTipo, ShoeVariantStock

CENT = Decimal('0.01')


def money(value):
    return Decimal(str(value or 0)).quantize(CENT, rounding=ROUND_HALF_UP)


def returned_quantity(db, item_id):
    return Decimal(str(db.query(func.coalesce(func.sum(CustomerReturn.cantidad), 0)).filter(CustomerReturn.item_id == item_id).scalar()))


def net_line_amount(item):
    # Allocate invoice rounding deterministically, so all returned lines sum to the paid invoice.
    lines = sorted(item.factura.items, key=lambda line: line.id)
    base = sum((Decimal(str(line.subtotal_cs or 0)) for line in lines), Decimal('0'))
    total = money(item.factura.total_cs)
    if base <= 0 or total <= 0:
        return Decimal('0')
    cumulative = Decimal('0')
    for line in lines:
        prior = cumulative
        cumulative += Decimal(str(line.subtotal_cs or 0))
        if line.id == item.id:
            return money(total * cumulative / base) - money(total * prior / base)
    return Decimal('0')


def return_amount(db, item, quantity):
    """Refund the actual discounted amount; the final return receives rounding residue."""
    sold = Decimal(str(item.cantidad or 0))
    returned = returned_quantity(db, item.id)
    if quantity <= 0 or quantity != quantity.to_integral_value() or quantity > sold - returned:
        raise ValueError('La cantidad debe ser entera y no superar las unidades disponibles para devolver.')
    net = net_line_amount(item)
    previous = money(db.query(func.coalesce(func.sum(CustomerReturn.monto_cs), 0)).filter(CustomerReturn.item_id == item.id).scalar())
    amount = net - previous if quantity == sold - returned else money(net * quantity / sold)
    if amount <= 0:
        raise ValueError('Este producto no tiene un importe pagado disponible para devolución.')
    return amount


def create_return(db, *, item_id, quantity, kind, reason, customer_name, operation_key, warehouse, allowed_ids, actor, now, rate):
    if not re.fullmatch(r'[0-9a-f]{32}', operation_key or ''):
        raise ValueError('Recarga la pantalla antes de confirmar la devolución.')
    # Same lock order as checkout / cash closing.
    db.query(Bodega).filter_by(id=warehouse.id).with_for_update().one()
    existing = db.query(CustomerReturn).filter_by(operation_key=operation_key).first()
    if existing:
        if existing.bodega_id != warehouse.id:
            raise ValueError('Esta operación pertenece a otra sucursal.')
        return existing
    item = db.get(VentaItem, item_id)
    if not item:
        raise ValueError('Producto facturado no encontrado.')
    invoice = db.query(VentaFactura).filter_by(id=item.factura_id).with_for_update().populate_existing().one()
    if invoice.bodega_id not in allowed_ids or invoice.estado != 'ACTIVA':
        raise ValueError('Factura no disponible para devolución en esta sucursal.')
    if invoice.condicion_venta != 'CONTADO' or invoice.estado_cobranza != 'PAGADA':
        raise ValueError('Esta etapa permite devoluciones de facturas de contado pagadas.')
    if not item.variante_id or item.producto.servicio_producto:
        raise ValueError('Solo se pueden devolver artículos de inventario con talla y color.')
    if kind == 'DINERO' and any(p.forma_pago and (p.forma_pago.nombre or '').strip().lower() == 'anticipo' for p in invoice.pagos):
        raise ValueError('Una compra pagada con anticipo se devuelve por canje; ese saldo no se convierte en efectivo.')
    if kind not in {'CANJE', 'DINERO'}:

        raise ValueError('Selecciona canje por producto o devolución de dinero.')
    reason = (reason or '').strip()
    if not reason or len(reason) > 300:
        raise ValueError('Escribe el motivo de la devolución (máximo 300 caracteres).')
    try:
        quantity = Decimal(str(quantity))
        if not quantity.is_finite():
            raise ValueError('Cantidad inválida.')
    except (InvalidOperation, TypeError):
        raise ValueError('Cantidad inválida.')
    amount = return_amount(db, item, quantity)
    customer = invoice.cliente
    if not customer or (customer.nombre or '').strip().lower() in {'consumidor final', 'cliente local'}:
        name = (customer_name or '').strip()
        if len(name) < 3 or len(name) > 160 or name.lower() in {'consumidor final', 'cliente local'}:
            raise ValueError('Identifica al cliente con su nombre para registrar la devolución.')
        customer = db.query(Cliente).filter(func.lower(Cliente.nombre) == name.lower()).first()
        if not customer:
            customer = Cliente(nombre=name, activo=True)
            db.add(customer); db.flush()
    tipo = db.query(IngresoTipo).filter_by(nombre='Devolución de cliente').first()
    if not tipo:
        # A transaction-level company lock avoids concurrent catalog creation across stores.
        if db.get_bind().dialect.name == 'postgresql':
            from sqlalchemy import text
            db.execute(text('SELECT pg_advisory_xact_lock(83742010)'))
            tipo = db.query(IngresoTipo).filter_by(nombre='Devolución de cliente').first()
        if not tipo:
            tipo = IngresoTipo(nombre='Devolución de cliente', requiere_proveedor=False)
            db.add(tipo); db.flush()
    cost = money(Decimal(str(item.producto.costo_producto or 0)) * quantity)
    ingreso = IngresoInventario(tipo_id=tipo.id, bodega_id=warehouse.id, fecha=now.date(), moneda='CS', tasa_cambio=rate or None,
        total_cs=cost, total_usd=money(cost / rate) if rate else 0, usuario_registro=actor,
        observacion=f'Devolución de factura {invoice.numero}: {reason}'[:300])
    db.add(ingreso); db.flush()
    db.add(IngresoItem(ingreso_id=ingreso.id, producto_id=item.producto_id, variante_id=item.variante_id,
        cantidad=quantity, costo_unitario_cs=item.producto.costo_producto or 0,
        costo_unitario_usd=money(Decimal(str(item.producto.costo_producto or 0))/rate) if rate else 0,
        subtotal_cs=cost, subtotal_usd=money(cost/rate) if rate else 0))
    stock = db.query(ShoeVariantStock).filter_by(variante_id=item.variante_id, bodega_id=warehouse.id).with_for_update().first()
    if not stock:
        stock = ShoeVariantStock(variante_id=item.variante_id, bodega_id=warehouse.id, existencia=0)
        db.add(stock)
    stock.existencia = Decimal(str(stock.existencia or 0)) + quantity
    result = CustomerReturn(operation_key=operation_key, factura_id=invoice.id, item_id=item.id, cliente_id=customer.id,
        bodega_id=warehouse.id, cantidad=quantity, monto_cs=amount, costo_cs=cost, tipo=kind, motivo=reason,
        usuario_registro=actor, created_at=now, ingreso_id=ingreso.id)
    db.add(result); db.flush()
    if kind == 'DINERO':
        for cls in (ReciboRubro, ReciboMotivo):
            row = db.query(cls).filter_by(nombre='Devolución de cliente').first()
            if not row:
                row = cls(nombre='Devolución de cliente', tipo='EGRESO', activo=True); db.add(row); db.flush()
            if cls is ReciboRubro: rubro = row
            else: motivo = row
        receipt = ReciboCaja(numero=f'DEV-{result.id:06d}', secuencia=result.id, branch_id=warehouse.branch_id,
            bodega_id=warehouse.id, tipo='EGRESO', rubro_id=rubro.id, motivo_id=motivo.id,
            descripcion=f'{result.numero} · Factura {invoice.numero} · {customer.nombre}'[:400], fecha=now.date(),
            moneda='CS', tasa_cambio=rate or None, monto_cs=amount, monto_usd=money(amount/rate) if rate else 0,
            afecta_caja=True, usuario_registro=actor)
        db.add(receipt); db.flush(); result.recibo_id = receipt.id
    return result


def consume_credits(db, *, payments, credit_ids, invoice, allowed_ids, now, actor):
    """Lock credits and consume whole values in the same sale transaction."""
    forms = {p.forma_pago_id: db.get(FormaPago, p.forma_pago_id) for p in payments}
    used = []
    for index, payment in enumerate(payments):
        form = forms[payment.forma_pago_id]
        credit_id = str(credit_ids[index] if index < len(credit_ids) else '').strip()
        is_credit = form and (form.nombre or '').strip().lower() == 'anticipo'
        if credit_id and not is_credit:
            raise ValueError('El anticipo debe utilizar la forma de pago Anticipo.')
        if is_credit:
            if not credit_id.isdigit():
                raise ValueError('Selecciona un anticipo de devolución; no se permite un monto manual.')
            used.append((int(credit_id), payment))
    if not used:
        return []
    if invoice.moneda != 'CS' or invoice.condicion_venta != 'CONTADO':
        raise ValueError('Los anticipos se canjean únicamente en facturas de contado en C$.')
    ids = [x[0] for x in used]
    if len(set(ids)) != len(ids):
        raise ValueError('El mismo anticipo no puede agregarse dos veces.')
    rows = db.query(CustomerReturn).filter(CustomerReturn.id.in_(ids)).order_by(CustomerReturn.id).with_for_update().populate_existing().all()
    credits = {r.id: r for r in rows}
    for ident, payment in used:
        credit = credits.get(ident)
        if not credit or credit.bodega_id not in allowed_ids or credit.tipo != 'CANJE' or credit.factura_canje_id:
            raise ValueError('Uno de los anticipos ya fue utilizado o no está disponible.')
        if credit.cliente_id != invoice.cliente_id:
            raise ValueError('El anticipo pertenece a otro cliente. Selecciona al cliente de la devolución.')
        if payment.moneda != 'CS' or payment.banco_id or payment.cuenta_id or money(payment.monto_cs) != money(credit.monto_cs) or money(payment.monto_original) != money(credit.monto_cs):
            raise ValueError('El anticipo debe aplicarse completo, sin cambiar el monto, en C$.')
    credit_total = sum((money(r.monto_cs) for r in rows), Decimal('0'))
    if credit_total > money(invoice.total_cs):
        raise ValueError('El nuevo producto debe costar igual o más que los anticipos. No se permite vuelto ni canje parcial.')
    # No cash-out through an inflated second payment when a credit is present.
    total_paid = sum((money(p.monto_cs) for p in payments), Decimal('0'))
    if total_paid != money(invoice.total_cs):
        raise ValueError('Con anticipos, agrega únicamente la diferencia exacta; no se permite vuelto.')
    for credit in rows:
        credit.factura_canje_id = invoice.id
        credit.canjeado_at = now
        credit.canjeado_por = actor
    return rows
