"""Embedded returns module; uses the host's authentication and store scope."""
from datetime import date, timedelta
from decimal import Decimal
import uuid
import re
from fastapi import Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, or_
from sqlalchemy.orm import selectinload
from ..models.user import User
from ..models.inventory import Bodega, Producto, ShoeProductVariant, ColorCatalog, ExchangeRate
from ..models.sales import Cliente, VentaFactura, VentaItem
from ..models.returns import CustomerReturn
from ..core.customer_returns import create_return, money, returned_quantity, return_amount, net_line_amount


def invoice_number_filter(value):
    """Match the whole numeric suffix, regardless of leading zeroes."""
    raw = re.sub(r"\s+", "", (value or "").strip().upper())
    match = re.fullmatch(r"(?:(?P<prefix>[A-Z]+)-?)?(?P<number>\d{1,12})", raw)
    if not match:
        return func.upper(VentaFactura.numero) == raw
    number = str(int(match.group('number')))
    prefix = match.group('prefix')
    if prefix:
        return (func.upper(func.substr(VentaFactura.numero, 1, len(prefix) + 1)) == prefix + '-') & (
            func.ltrim(func.substr(VentaFactura.numero, len(prefix) + 2), '0') == number)
    return (func.upper(func.substr(VentaFactura.numero, 1, 2)).in_(['A-', 'B-', 'C-'])) & (
        func.ltrim(func.substr(VentaFactura.numero, 3), '0') == number)


def register(router):
    from . import web as w
    get_db = w.get_db

    def access(request, user):
        if not w._is_shoes_mode():
            raise HTTPException(404, 'Disponible únicamente en Miss Zapatos')
        w._enforce_permission(request, user, 'access.sales.devoluciones')

    def scope(db, user):
        branch, warehouse = w._quick_transfer_branch_bodega(db, user)
        if not warehouse:
            raise HTTPException(403, 'Necesitas una sucursal y bodega asignadas')
        ids = {b.id for b in w._scoped_bodegas_query(db).all()}
        if not w._shoe_admin_all_stores(user) and (branch.code or '').lower() != 'central':
            ids &= {warehouse.id}
        return warehouse, ids

    def payload(row):
        item = row.item
        return dict(id=row.id, numero=row.numero, factura=row.factura.numero, cliente_id=row.cliente_id,
            cliente=row.cliente.nombre, fecha=row.created_at.strftime('%d/%m/%Y'), hora=row.created_at.strftime('%H:%M'),
            producto=item.producto.descripcion, codigo=item.producto.cod_producto,
            variante=item.variante.cod_variante if item.variante else '', cantidad=float(row.cantidad),
            monto_cs=float(row.monto_cs), tipo=row.tipo, estado=row.estado, motivo=row.motivo,
            usuario=row.usuario_registro, bodega=row.bodega.name, factura_canje=row.factura_canje.numero if row.factura_canje else '',
            fecha_canje=row.canjeado_at.strftime('%d/%m/%Y %H:%M') if row.canjeado_at else '',
            usuario_canje=row.canjeado_por or '', recibo=row.recibo.numero if row.recibo else '')

    @router.get('/sales/devoluciones')
    def page(request: Request, db=Depends(get_db), user: User=Depends(w._require_admin_web)):
        access(request, user)
        warehouse, _ = scope(db, user)
        return request.app.state.templates.TemplateResponse('sales_devoluciones.html', dict(
            request=request, user=user, bodega=warehouse, operation_key=uuid.uuid4().hex,
            history_from=(w.local_today()-timedelta(days=29)).isoformat(), history_to=w.local_today().isoformat(), version=w.settings.UI_VERSION))

    @router.get('/sales/devoluciones/facturas')
    def search(request: Request, numero: str='', cliente: str='', fecha: str='', producto: str='', db=Depends(get_db), user: User=Depends(w._require_admin_web)):
        access(request, user)
        _, ids = scope(db, user)
        query = db.query(VentaItem).join(VentaFactura).join(Producto).outerjoin(Cliente, Cliente.id == VentaFactura.cliente_id).outerjoin(ShoeProductVariant, ShoeProductVariant.id == VentaItem.variante_id).outerjoin(ColorCatalog, ColorCatalog.id == ShoeProductVariant.color_id).filter(VentaFactura.bodega_id.in_(ids), VentaFactura.estado == 'ACTIVA')
        if not any(x.strip() for x in [numero, cliente, fecha, producto]):
            return JSONResponse(dict(ok=True, items=[], message='Escribe el número de factura, con o sin ceros, o utiliza los filtros.'))
        if numero.strip(): query = query.filter(invoice_number_filter(numero))
        if cliente.strip(): query = query.filter(Cliente.nombre.ilike('%'+cliente.strip().replace('%',r'\%').replace('_',r'\_')+'%', escape='\\'))
        if fecha:
            try: query = query.filter(func.date(VentaFactura.fecha) == date.fromisoformat(fecha))
            except ValueError: return JSONResponse(dict(ok=False, message='Fecha inválida'), status_code=400)
        invoice_query = query
        for token in producto.split()[:8]:
            term = '%'+token.replace('%',r'\%').replace('_',r'\_')+'%'
            query = query.filter(or_(Producto.cod_producto.ilike(term, escape='\\'), Producto.descripcion.ilike(term, escape='\\'), ShoeProductVariant.cod_variante.ilike(term, escape='\\'), ShoeProductVariant.talla.ilike(term, escape='\\'), ColorCatalog.nombre.ilike(term, escape='\\')))
        rows = query.order_by(VentaFactura.fecha.desc(), VentaItem.id).limit(101).all()
        if not rows and producto.strip():
            from difflib import SequenceMatcher
            tokens = w._ascii_lower(producto).split()[:8]
            candidates = invoice_query.order_by(VentaFactura.fecha.desc(), VentaItem.id).limit(500).all()
            def matches(item):
                variant = item.variante
                words = w._ascii_lower(" ".join([item.producto.cod_producto, item.producto.descripcion,
                    variant.cod_variante if variant else "", variant.talla if variant else "",
                    variant.color.nombre if variant and variant.color else ""])).split()
                return all(any(token in word or (len(token) >= 4 and SequenceMatcher(None, token, word).ratio() >= .78)
                               for word in words) for token in tokens)
            rows = [item for item in candidates if matches(item)][:101]

        result = []
        for item in rows[:100]:
            inv = item.factura
            available = Decimal(str(item.cantidad or 0)) - returned_quantity(db, item.id)
            net = net_line_amount(item)
            unit = money(net/Decimal(str(item.cantidad))) if item.cantidad else 0
            eligible = available > 0 and unit > 0 and item.variante_id and inv.condicion_venta == 'CONTADO' and inv.estado_cobranza == 'PAGADA'
            result.append(dict(item_id=item.id, factura_id=inv.id, factura=inv.numero,
                fecha=inv.fecha.strftime('%d/%m/%Y'), hora=inv.fecha.strftime('%H:%M'), cliente=inv.cliente.nombre if inv.cliente else 'Consumidor final',
                cliente_id=inv.cliente_id, codigo=item.producto.cod_producto, producto=item.producto.descripcion,
                variante=item.variante.cod_variante if item.variante else '', color=item.variante.color.nombre if item.variante and item.variante.color else '',
                talla=item.variante.talla if item.variante else '', cantidad=float(item.cantidad), disponible=float(available),
                precio_cs=float(item.precio_unitario_cs or 0), neto_unitario_cs=float(unit), total_factura_cs=float(inv.total_cs or 0),
                elegible=bool(eligible), solo_canje=any(p.forma_pago and (p.forma_pago.nombre or '').strip().lower()=='anticipo' for p in inv.pagos), total_linea_cs=float(net), remaining_cs=float(net-money(db.query(func.coalesce(func.sum(CustomerReturn.monto_cs),0)).filter(CustomerReturn.item_id==item.id).scalar()))))
        return JSONResponse(dict(ok=True, items=result, more=len(rows)>100))

    @router.post('/sales/devoluciones')
    async def submit(request: Request, db=Depends(get_db), user: User=Depends(w._require_admin_web)):
        access(request, user)
        warehouse, ids = scope(db, user)
        form = await request.form()
        try:
            # Recheck the opening while holding the same warehouse lock as checkout and closing.
            db.query(Bodega).filter_by(id=warehouse.id).with_for_update().one()
            opening = w._shoe_cash_opening(db, warehouse.id, w.local_today())
            if not opening or opening.cierre_id:
                raise ValueError('Abre la caja de hoy antes de aplicar una devolución.')
            rate = db.query(ExchangeRate).filter(ExchangeRate.effective_date <= w.local_today()).order_by(ExchangeRate.effective_date.desc()).first()
            row = create_return(db, item_id=int(form.get('item_id') or 0), quantity=form.get('cantidad'), kind=form.get('tipo'),
                reason=form.get('motivo'), customer_name=form.get('cliente_nombre'), operation_key=form.get('operation_key'),
                warehouse=warehouse, allowed_ids=ids, actor=user.full_name or user.email, now=w.local_now_naive(),
                rate=Decimal(str(rate.rate)) if rate else Decimal('0'))
            w._post_return_accounting(db, row)
            db.commit()
            return JSONResponse(dict(ok=True, devolucion=payload(row), operation_key=uuid.uuid4().hex))
        except (ValueError, TypeError) as error:
            db.rollback()
            return JSONResponse(dict(ok=False, message=str(error)), status_code=400)

    @router.get('/sales/devoluciones/historial')
    def history(request: Request, q: str='', estado: str='', desde: str='', hasta: str='', page: int=1, db=Depends(get_db), user: User=Depends(w._require_admin_web)):
        access(request, user)
        _, ids = scope(db, user)
        query = db.query(CustomerReturn).join(VentaFactura, CustomerReturn.factura_id == VentaFactura.id).join(Cliente, CustomerReturn.cliente_id == Cliente.id).filter(CustomerReturn.bodega_id.in_(ids))
        if q.strip():
            term='%'+q.strip().replace('%',r'\%').replace('_',r'\_')+'%'
            query=query.filter(or_(Cliente.nombre.ilike(term, escape='\\'), VentaFactura.numero.ilike(term, escape='\\'), CustomerReturn.motivo.ilike(term, escape='\\')))
        if estado == 'DISPONIBLE': query=query.filter(CustomerReturn.tipo=='CANJE', CustomerReturn.factura_canje_id.is_(None))
        if estado == 'APLICADO': query=query.filter(CustomerReturn.factura_canje_id.is_not(None))
        if estado == 'DINERO': query=query.filter(CustomerReturn.tipo=='DINERO')
        if not desde and not hasta:
            desde = (w.local_today()-timedelta(days=29)).isoformat()
            hasta = w.local_today().isoformat()
        try:
            if desde and hasta and date.fromisoformat(desde) > date.fromisoformat(hasta):
                return JSONResponse(dict(ok=False, message='La fecha desde debe ser anterior o igual a hasta.'), status_code=400)
            if desde: query=query.filter(func.date(CustomerReturn.created_at)>=date.fromisoformat(desde))
            if hasta: query=query.filter(func.date(CustomerReturn.created_at)<=date.fromisoformat(hasta))
        except ValueError: return JSONResponse(dict(ok=False, message='Fecha inválida'), status_code=400)
        count=query.count();page=max(1,page)
        rows=query.order_by(CustomerReturn.created_at.desc(),CustomerReturn.id.desc()).offset((page-1)*30).limit(30).all()
        return JSONResponse(dict(ok=True, items=[payload(r) for r in rows], total=count, page=page, more=page*30<count))

    @router.get('/sales/devoluciones/anticipos')
    def credits(request: Request, q: str='', cliente_id: str='', db=Depends(get_db), user: User=Depends(w._require_admin_web)):
        if not w._is_shoes_mode(): raise HTTPException(404)
        w._enforce_permission(request, user, 'access.sales.registrar')
        _, ids=scope(db, user)
        query=db.query(CustomerReturn).join(VentaFactura, CustomerReturn.factura_id==VentaFactura.id).join(Cliente, CustomerReturn.cliente_id==Cliente.id).filter(CustomerReturn.bodega_id.in_(ids), CustomerReturn.tipo=='CANJE', CustomerReturn.factura_canje_id.is_(None))
        if cliente_id.isdigit(): query=query.filter(CustomerReturn.cliente_id==int(cliente_id))
        elif not q.strip(): return JSONResponse(dict(ok=True, items=[]))
        if q.strip():
            term='%'+q.strip().replace('%',r'\%').replace('_',r'\_')+'%'
            conditions=[invoice_number_filter(q), Cliente.nombre.ilike(term, escape='\\')]
            if q.strip().upper().startswith('DEV-') and q.strip()[4:].isdigit(): conditions.append(CustomerReturn.id==int(q.strip()[4:]))
            query=query.filter(or_(*conditions))
        rows=query.order_by(CustomerReturn.id).limit(100).all()
        return JSONResponse(dict(ok=True, items=[payload(r) for r in rows]))
