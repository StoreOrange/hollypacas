"""Ingresos de variantes individuales, sin distribución por cajones."""
import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from uuid import UUID, uuid4
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, text, or_, case
from sqlalchemy.exc import IntegrityError
from . import web as w

router = APIRouter()
PATH = '/inventory/ingresos-unidad'


def money(value):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError('Importe inválido')
    if not number.is_finite() or number < 0 or number > Decimal('99999999') or number != number.quantize(Decimal('.01')):
        raise ValueError('Los importes deben ser positivos y tener máximo dos decimales')
    return number


def validate_rows(rows):
    if not isinstance(rows, list) or not 1 <= len(rows) <= 100:
        raise ValueError('Agrega entre 1 y 100 filas')
    result = []
    prices = {}
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Fila inválida")
        code = str(row.get('code', '')).strip().upper()
        scan = str(row.get('scan_code', '')).strip().upper()
        size = str(row.get('size', '')).strip()
        color = str(row.get('color', '')).strip()
        name = str(row.get('name', '')).strip()
        brand = str(row.get('brand', '')).strip() or 'Sin Marca'
        if not re.fullmatch(r'[A-Z0-9][A-Z0-9._-]{0,59}', code):
            raise ValueError('Código de modelo inválido: usa letras, números, punto, guion o guion bajo')
        if scan and not re.fullmatch(r'[A-Z0-9][A-Z0-9._-]{0,79}', scan):
            raise ValueError('Código de etiqueta inválido')
        if not name or len(name) > 200 or not color or len(color) > 120 or not size or len(size) > 20 or len(brand) > 80:
            raise ValueError('Revisa descripción, marca, color y talla de cada fila')
        qty = Decimal(str(row.get('qty', 0)))
        if not qty.is_finite() or qty != qty.to_integral_value() or not 1 <= qty <= 10000:
            raise ValueError('La cantidad debe ser un número entero entre 1 y 10,000')
        cost, price = money(row.get('cost', 0)), money(row.get('price', 0))
        if price <= 0 or cost >= price:
            raise ValueError('El precio debe ser mayor que cero y mayor que el costo')
        if code in prices and prices[code] != (cost, price):
            raise ValueError('Las filas del mismo modelo deben tener el mismo costo y precio')
        prices[code] = (cost, price)
        key = (code, color.casefold(), size)
        if key in seen:
            raise ValueError('La misma talla/color está repetida; aumenta la cantidad en su fila')
        seen.add(key)
        result.append(dict(code=code, scan_code=scan, name=name, brand=brand, color=color, size=size,
                           line_id=int(row.get('line_id', 0)), qty=qty, cost=cost, price=price))
    return result


def access(request, user):
    w._enforce_permission(request, user, 'access.inventory.ingresos')
    if not w._is_shoes_mode():
        raise HTTPException(404, 'Disponible únicamente en Miss Zapatos')


def allowed_warehouses(db, user):
    query = db.query(w.Bodega)
    # Misma política de sucursales que las utilidades de Miss Zapatos:
    # administrador en todas las tiendas; demás usuarios en su ámbito asignado.
    branches = w._utility_branch_ids(db, user)
    query = query.filter(w.Bodega.branch_id.in_(branches), w.Bodega.activo.is_(True))
    return query.order_by(w.Bodega.name).all()


@router.get(PATH)
def page(request: Request, db=Depends(w.get_db), user=Depends(w._require_admin_web)):
    access(request, user)
    warehouses = allowed_warehouses(db, user)
    origin = str(request.query_params.get('bodega_id') or '').strip()
    if origin:
        if not origin.isdigit():
            raise HTTPException(400, 'Bodega de origen inválida')
        selected = next((b for b in warehouses if b.id == int(origin) and b.activo), None)
        if not selected:
            raise HTTPException(403, 'No tiene acceso a la bodega de origen seleccionada')
        selected_id = selected.id
    else:
        selected_id = user.default_bodega_id
    return request.app.state.templates.TemplateResponse('shoe_unit_receipts.html', dict(
        request=request, user=user, version=w.settings.UI_VERSION, today=w.local_today(), token=str(uuid4()),
        embedded=request.query_params.get('embed') == '1', initial_query=str(request.query_params.get('q') or '').strip()[:120],
        warehouses=warehouses, selected_warehouse_id=selected_id, types=[t for t in db.query(w.IngresoTipo).all() if 'traslado' not in w._ascii_lower(t.nombre)],
        suppliers=db.query(w.Proveedor).all(), lines=db.query(w.Linea).filter(w.Linea.activo.is_(True)).all(),
        colors=db.query(w.ColorCatalog).filter(w.ColorCatalog.activo.is_(True)).all()))


def search_value(column):
    return func.lower(func.translate(func.coalesce(column, ''), 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN'))


def product_payload(product, variant=None):
    return dict(code=product.cod_producto, name=product.descripcion, brand=product.marca,
        line_id=product.linea_id, cost=str(product.costo_producto or 0), price=str(product.precio_venta1 or 0),
        scan_code=variant.cod_variante if variant else '', size=variant.talla if variant else '',
        color=variant.color.nombre if variant else '', kind='variant' if variant else 'model')


@router.get(PATH + '/search')
def search(request: Request, q: str = '', db=Depends(w.get_db), user=Depends(w._require_admin_web)):
    access(request, user)
    query = q.strip()[:120]
    if len(query) < 2:
        return dict(items=[])
    tokens = w._tokenize_search(query)[:8]
    if not tokens:
        return dict(items=[])
    def conditions(columns):
        return [or_(*[search_value(col).contains(token, autoescape=True) for col in columns]) for token in tokens]
    models = db.query(w.Producto).filter(w.Producto.activo.is_(True), *conditions([
        w.Producto.cod_producto, w.Producto.descripcion, w.Producto.marca])).order_by(
        case((func.lower(w.Producto.cod_producto) == query.lower(), 0), else_=1), w.Producto.cod_producto).limit(12).all()
    variants_query = db.query(w.ShoeProductVariant).join(w.Producto, w.Producto.id == w.ShoeProductVariant.producto_id).join(w.ColorCatalog, w.ColorCatalog.id == w.ShoeProductVariant.color_id).filter(w.Producto.activo.is_(True), w.ShoeProductVariant.activo.is_(True))
    if re.fullmatch(r'V\d{1,10}', query.upper()):
        variants_query = variants_query.filter(w.ShoeProductVariant.id == int(query[1:]))
    else:
        variants_query = variants_query.filter(*conditions([w.ShoeProductVariant.cod_variante,
            w.Producto.cod_producto, w.Producto.descripcion, w.Producto.marca, w.ColorCatalog.nombre, w.ShoeProductVariant.talla]))
    variants = variants_query.order_by(case((func.lower(w.ShoeProductVariant.cod_variante) == query.lower(), 0), else_=1), w.ShoeProductVariant.cod_variante).limit(12).all()
    items = [product_payload(product) for product in models] + [product_payload(variant.producto, variant) for variant in variants]
    def rank(item):
        exact = query.lower() in [item['code'].lower(), item['scan_code'].lower()]
        return (not exact, item['kind'] != 'model', item['code'], item['scan_code'])
    items.sort(key=rank)
    return dict(items=items[:12], limited=len(items) >= 12)


@router.get(PATH + '/lookup')
def lookup(request: Request, code: str, db=Depends(w.get_db), user=Depends(w._require_admin_web)):
    access(request, user)
    product = db.query(w.Producto).filter(func.lower(w.Producto.cod_producto) == code.strip().lower()).first()
    variant = db.query(w.ShoeProductVariant).filter(w.ShoeProductVariant.id == int(code.strip()[1:])).first() if re.fullmatch(r'V\d{1,10}', code.strip().upper()) else db.query(w.ShoeProductVariant).filter(func.lower(w.ShoeProductVariant.cod_variante) == code.strip().lower()).first()
    if variant:
        product = variant.producto
    if not product:
        return dict(found=False)
    return dict(found=True, code=product.cod_producto, name=product.descripcion, brand=product.marca,
                line_id=product.linea_id, cost=str(product.costo_producto or 0), price=str(product.precio_venta1 or 0),
                scan_code=variant.cod_variante if variant else '', size=variant.talla if variant else '',
                color=variant.color.nombre if variant else '')


@router.post(PATH)
async def create(request: Request, db=Depends(w.get_db), user=Depends(w._require_admin_web)):
    access(request, user)
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise ValueError("Solicitud inválida")
        rows = validate_rows(payload.get('rows'))
        token = str(UUID(payload.get('token', '')))
        entry_date = date.fromisoformat(payload.get('date', ''))
        warehouse_id = int(payload.get('warehouse_id', 0))
        warehouse = next((b for b in allowed_warehouses(db, user) if b.id == warehouse_id), None)
        if not warehouse:
            raise ValueError('No tiene acceso a la bodega seleccionada')
        movement_type = db.query(w.IngresoTipo).get(int(payload.get('type_id', 0)))
        supplier_id = int(payload.get('supplier_id') or 0) or None
        if movement_type and 'traslado' in w._ascii_lower(movement_type.nombre):
            raise ValueError('Para mover existencias entre bodegas utiliza Traslados rápidos')
        if not movement_type or (movement_type.requiere_proveedor and not supplier_id):
            raise ValueError('Selecciona un tipo de ingreso y su proveedor cuando sea requerido')
        if supplier_id and not db.query(w.Proveedor).get(supplier_id):
            raise ValueError('Proveedor inválido')
        if db.get_bind().dialect.name == 'postgresql':
            db.execute(text('SELECT pg_advisory_xact_lock(83742019)'))
        marker = 'Ingreso por unidad [' + token + ']'
        prior = db.query(w.IngresoInventario).filter(w.IngresoInventario.observacion.like(marker + '%')).first()
        if prior:
            if prior.bodega_id != warehouse_id:
                raise ValueError('Este registro ya pertenece a otra bodega')
            return dict(ok=True, receipt_id=prior.id, labels_url=f'/inventory/ingresos/{prior.id}/labels/preview')
        receipt = w.IngresoInventario(tipo_id=movement_type.id, bodega_id=warehouse_id, proveedor_id=supplier_id,
              fecha=entry_date, moneda='CS', usuario_registro=user.full_name,
              observacion=(marker + ' ' + str(payload.get('note') or ''))[:300])
        db.add(receipt); db.flush()
        rate = db.query(w.ExchangeRate).filter(w.ExchangeRate.effective_date <= w.local_today()).order_by(w.ExchangeRate.effective_date.desc()).first()
        exchange = Decimal(str(rate.rate)) if rate else Decimal(0)
        if exchange <= 0:
            raise ValueError('Registra una tasa de cambio válida antes del ingreso')
        total = Decimal(0)
        total_usd = Decimal(0)
        for row in rows:
            line = db.query(w.Linea).filter(w.Linea.id == row['line_id'], w.Linea.activo.is_(True)).first()
            if not line:
                raise ValueError('Línea de producto inválida')
            color = db.query(w.ColorCatalog).filter(func.lower(w.ColorCatalog.nombre) == row['color'].lower()).first()
            if not color:
                color = w.ColorCatalog(nombre=row['color'], abreviatura=w._sanitize_code_token(row['color'], 20), activo=True)
                db.add(color); db.flush()
            if not color.activo:
                raise ValueError('El color seleccionado está inactivo')
            product = db.query(w.Producto).filter(func.lower(w.Producto.cod_producto) == row['code'].lower()).with_for_update().first()
            if not product:
                product = w.Producto(cod_producto=row['code'], descripcion=row['name'], linea_id=line.id,
                       marca=row['brand'], activo=True, usuario_registro=user.full_name)
                db.add(product); db.flush()
            elif not product.activo:
                raise ValueError('El modelo está inactivo; actívalo en el catálogo')
            variant = db.query(w.ShoeProductVariant).filter_by(producto_id=product.id, color_id=color.id, talla=row['size']).first()
            scan = row['scan_code'] or (variant.cod_variante if variant else f"{row['code']}-{w._sanitize_code_token(color.abreviatura, 12)}-{w._sanitize_code_token(row['size'], 6)}")
            other_model = db.query(w.Producto).filter(func.lower(w.Producto.cod_producto) == scan.lower(), w.Producto.id != product.id).first()
            if other_model:
                raise ValueError('El código de etiqueta pertenece a otro modelo: ' + scan)
            collision = db.query(w.ShoeProductVariant).filter(func.lower(w.ShoeProductVariant.cod_variante) == scan.lower()).first()
            if collision and (not variant or collision.id != variant.id):
                raise ValueError('El código de etiqueta ya pertenece a otra variante: ' + scan)
            if variant and variant.cod_variante != scan:
                raise ValueError('Esta talla/color ya tiene código ' + variant.cod_variante + '; usa ese código')
            if not variant:
                variant = w.ShoeProductVariant(producto_id=product.id, color_id=color.id, talla=row['size'], cod_variante=scan, activo=True)
                db.add(variant); db.flush()
            variant.activo = True
            for tier in range(1, 8):
                price = max(Decimal(0), row['price'] - (tier - 1) * Decimal(10))
                setattr(product, 'precio_venta' + str(tier), price)
                setattr(product, 'precio_venta' + str(tier) + '_usd', (price / exchange).quantize(Decimal('.0001'), rounding=ROUND_HALF_UP))
            product.costo_producto = row['cost']; product.tasa_cambio = exchange
            stock = db.query(w.ShoeVariantStock).filter_by(variante_id=variant.id, bodega_id=warehouse_id).with_for_update().first()
            if not stock:
                stock = w.ShoeVariantStock(variante_id=variant.id, bodega_id=warehouse_id, existencia=0); db.add(stock)
            stock.existencia = Decimal(str(stock.existencia or 0)) + row['qty']
            balance = db.query(w.SaldoProducto).filter_by(producto_id=product.id).with_for_update().first()
            if not balance:
                balance = w.SaldoProducto(producto_id=product.id, existencia=0); db.add(balance)
            balance.existencia = Decimal(str(balance.existencia or 0)) + row['qty']
            subtotal = row['cost'] * row['qty']
            usd = (subtotal / exchange).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
            db.add(w.IngresoItem(ingreso_id=receipt.id, producto_id=product.id, variante_id=variant.id,
                 cantidad=row['qty'], costo_unitario_cs=row['cost'], costo_unitario_usd=row['cost']/exchange,
                 subtotal_cs=subtotal, subtotal_usd=usd))
            total += subtotal; total_usd += usd
            db.flush()
        receipt.total_cs = total; receipt.total_usd = total_usd
        entry = w._build_auto_accounting_entry(db, event_code='INV_IN', branch_id=warehouse.branch_id,
            entry_date=entry_date, amount=total, reference=f'AUTO-ING-UND-{receipt.id}',
            description=f'Ingreso por unidad #{receipt.id}')
        if entry: db.add(entry)
        db.commit()
        return dict(ok=True, receipt_id=receipt.id, labels_url=f'/inventory/ingresos/{receipt.id}/labels/preview')
    except (ValueError, TypeError, InvalidOperation) as exc:
        db.rollback()
        return JSONResponse(dict(ok=False, error=str(exc)), status_code=400)
    except HTTPException as exc:
        db.rollback()
        return JSONResponse(dict(ok=False, error=str(exc.detail)), status_code=exc.status_code)
    except IntegrityError:
        db.rollback()
        return JSONResponse(dict(ok=False, error='Otro registro creó el código o la variante al mismo tiempo. Consulta el código y reintenta.'), status_code=409)
    except Exception:
        db.rollback()
        raise
