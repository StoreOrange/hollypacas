"""Requisitions use the inventory ledger, warehouse locks and atomic paired docs."""
import html
import json
import re
from decimal import Decimal
from fastapi import HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import selectinload
from ..models.inventory_requests import InventoryRequest, InventoryRequestItem, InventoryRequestEvent
from ..models.inventory import Bodega, Producto, IngresoInventario, IngresoItem, IngresoTipo, EgresoInventario, EgresoItem, EgresoTipo
from ..routers import web as w

PATH = '/sales/requisas'
STATUSES = {'pending', 'active', 'cancelled'}


def is_admin(user):
    return not w._restricted_motorizado(user) and any(r.name == 'administrador' for r in user.roles or [])


def enforce(user):
    if not w._is_pacasholl_company():
        raise HTTPException(404, 'Módulo exclusivo de Pacas Holl')
    roles = {r.name for r in user.roles or []}
    if w._restricted_motorizado(user) or not (is_admin(user) or roles & {'cajero', 'vendedor'} or w._has_permission(user, 'access.sales')):
        raise HTTPException(403, 'Tu usuario no tiene acceso a las requisas de ventas')


def warehouses(db):
    rows = db.query(Bodega).join(w.Branch).filter(Bodega.activo.is_(True), w.Branch.activo.is_(True), func.lower(Bodega.code).in_(['central','esteli']), func.lower(w.Branch.code).in_(['central','esteli'])).all()
    result = {r.code.lower(): r for r in rows if r.code.lower() == r.branch.code.lower()}
    if len(result) != 2:
        raise HTTPException(409, 'Central y Estelí deben tener sus bodegas principales activas')
    return result


def destination(db, user, raw=None):
    enforce(user)
    pair = warehouses(db)
    _, current = w._resolve_branch_bodega(db, user)
    if raw and is_admin(user):
        selected = next((b for b in pair.values() if str(b.id) == str(raw)), None)
    else:
        selected = next((b for b in pair.values() if current and b.id == current.id), None)
        if not selected and is_admin(user):
            selected = pair['central']
        if raw and selected and str(selected.id) != str(raw):
            raise HTTPException(403, 'Solo puedes solicitar para tu bodega asignada')
    if not selected:
        raise HTTPException(403, 'Selecciona Central o Estelí como bodega operativa')
    other = next(b for b in pair.values() if b.id != selected.id)
    return selected, other


def query(db, user):
    enforce(user)
    q = db.query(InventoryRequest)
    if not is_admin(user):
        dest, _ = destination(db, user)
        q = q.filter(InventoryRequest.destination_id == dest.id)
    return q


def get(db, user, ident, lock=False):
    q = query(db, user).filter(InventoryRequest.id == ident)
    if lock:
        q = q.populate_existing().with_for_update()
    row = q.first()
    if not row:
        raise HTTPException(404, 'Solicitud no encontrada')
    return row


def event(db, record, user, action, detail=''):
    db.add(InventoryRequestEvent(request=record, user_id=user.id, user_name=user.full_name or user.email, action=action, detail=detail, created_at=w.local_now_naive()))


def parse_items(raw):
    if isinstance(raw,str) and len(raw)>20000:
        raise ValueError('La lista de productos es demasiado grande')
    try:
        values = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        raise ValueError('Revisa los productos solicitados')
    if not isinstance(values, list) or not 1 <= len(values) <= 60:
        raise ValueError('Selecciona entre 1 y 60 productos')
    result = {}
    for value in values:
        if not isinstance(value, dict):
            raise ValueError('Producto inválido')
        pid, qty = str(value.get('id','')), str(value.get('quantity',''))
        if not re.fullmatch(r'[1-9][0-9]{0,8}',pid) or not re.fullmatch(r'[1-9][0-9]{0,6}',qty) or int(qty)>1000000:
            raise ValueError('Las cantidades deben ser números enteros mayores que cero, hasta 1,000,000')
        if int(pid) in result:
            raise ValueError('Cada producto debe aparecer una sola vez')
        result[int(pid)] = int(qty)
    return result


def create(db, user, operation, destination_id, seller_id, raw_items, observation, review_base):
    dest, source = destination(db, user, destination_id)
    previous = db.query(InventoryRequest).filter_by(operation_key=operation,requested_by=user.id).first()
    if previous:
        return previous, False
    sellers = w._vendedores_for_bodega(db, dest)
    seller = next((s for s in sellers if str(s.id) == str(seller_id)), None)
    if not seller:
        raise ValueError('Selecciona un vendedor de la bodega solicitante')
    items = parse_items(raw_items)
    observation = str(observation or '').strip()
    if len(observation)>1000:
        raise ValueError('La observación puede tener hasta 1,000 caracteres')
    products = {p.id:p for p in db.query(Producto).filter(Producto.id.in_(items),Producto.activo.is_(True),Producto.servicio_producto.is_(False)).all()}
    if set(products)!=set(items):
        raise ValueError('Uno de los productos ya no está disponible para solicitar')
    record = InventoryRequest(operation_key=operation,fecha=w.local_today(),created_at=w.local_now_naive(),source_id=source.id,destination_id=dest.id,requested_by=user.id,requester_name=user.full_name or user.email,vendedor_id=seller.id,seller_name=seller.nombre,status='pending',observation=observation,review_url=review_base,decision_reason='',email_state='pending',email_error='')
    db.add(record)
    for pid, qty in items.items():
        p = products[pid]
        record.items.append(InventoryRequestItem(producto_id=pid,code=p.cod_producto,description=p.descripcion,quantity=qty))
    event(db,record,user,'Solicitud enviada',f'{source.name} → {dest.name}')
    db.flush()
    record.review_url = review_base.rstrip('/') + f'{PATH}/{record.id}'
    return record, True


def balances(db, record):
    return w._balances_by_bodega(db,[record.source_id,record.destination_id],[i.producto_id for i in record.items])


def cancel(db, record, user, reason):
    if record.status != 'pending':
        return record
    if not is_admin(user) and record.requested_by != user.id:
        raise HTTPException(403, 'Solo el administrador o el solicitante pueden anularla')
    reason = str(reason or '').strip()
    if not 3 <= len(reason) <= 1000:
        raise ValueError('Indica un motivo de anulación entre 3 y 1,000 caracteres')
    record.status='cancelled';record.decision_reason=reason
    record.decided_by=user.id;record.decider_name=user.full_name or user.email;record.decided_at=w.local_now_naive()
    event(db,record,user,'Solicitud anulada',reason)
    return record


def approve(db, user, ident):
    enforce(user)
    if not is_admin(user):
        raise HTTPException(403, 'Solo el administrador puede autorizar')
    record = get(db,user,ident,lock=True)
    if record.status != 'pending':
        return record
    # Same warehouse row locks used by sales; stable order prevents opposite-route deadlocks.
    locked = db.query(Bodega).filter(Bodega.id.in_([record.source_id,record.destination_id])).order_by(Bodega.id).with_for_update().all()
    pair = warehouses(db)
    if {b.id for b in locked} != {b.id for b in pair.values()}:
        return cancel(db,record,user,'La ruta ya no corresponde a Central y Estelí activas. Registra otra solicitud.')
    stock = balances(db,record)
    products = {p.id:p for p in db.query(Producto).filter(Producto.id.in_([i.producto_id for i in record.items])).all()}
    missing=[]
    for item in record.items:
        p=products.get(item.producto_id)
        available=stock.get((item.producto_id,record.source_id),Decimal(0))
        if not p or not p.activo or p.servicio_producto or available < item.quantity:
            missing.append(f'{item.code}: solicita {item.quantity}, disponible {available:g}')
    if missing:
        return cancel(db,record,user,('Saldo insuficiente o producto no disponible en '+record.source.name+'. '+ '; '.join(missing)+'. Registra una nueva solicitud.')[:1000])
    out_type=db.query(EgresoTipo).filter(func.lower(EgresoTipo.nombre)=='traslado entre bodegas').first()
    in_type=db.query(IngresoTipo).filter(func.lower(IngresoTipo.nombre)=='traslado entre bodegas').first()
    if not out_type:
        out_type=EgresoTipo(nombre='Traslado entre bodegas');db.add(out_type)
    if not in_type:
        in_type=IngresoTipo(nombre='Traslado entre bodegas',requiere_proveedor=False);db.add(in_type)
    db.flush()
    total=sum((Decimal(str(products[i.producto_id].costo_producto or 0))*i.quantity for i in record.items),Decimal(0))
    name=user.full_name or user.email
    note=f'{record.numero}. Solicitó: {record.requester_name}. Autorizó: {name}.'
    out=EgresoInventario(tipo_id=out_type.id,bodega_id=record.source_id,bodega_destino_id=record.destination_id,fecha=w.local_today(),moneda='CS',total_cs=total,total_usd=0,observacion=note[:300],usuario_registro=name)
    db.add(out);db.flush()
    receipt=IngresoInventario(tipo_id=in_type.id,bodega_id=record.destination_id,fecha=w.local_today(),moneda='CS',total_cs=total,total_usd=0,observacion=f'Traslado desde {record.source.name} hacia {record.destination.name}. Egreso #{out.id} | {note}'[:300],usuario_registro=name)
    db.add(receipt);db.flush()
    for item in record.items:
        cost=Decimal(str(products[item.producto_id].costo_producto or 0))
        values=dict(producto_id=item.producto_id,cantidad=item.quantity,costo_unitario_cs=cost,costo_unitario_usd=0,subtotal_cs=cost*item.quantity,subtotal_usd=0)
        db.add(EgresoItem(egreso_id=out.id,**values));db.add(IngresoItem(ingreso_id=receipt.id,**values))
        item.cost_cs=cost
    # Auxiliary product stock is global: paired transfer has a net change of zero.
    record.egreso_id=out.id;record.ingreso_id=receipt.id;record.status='active'
    record.decided_by=user.id;record.decider_name=name;record.decided_at=w.local_now_naive()
    event(db,record,user,'Autorizada y trasladada',f'Egreso #{out.id} · Ingreso #{receipt.id}. {record.source.name} → {record.destination.name}')
    db.flush()
    return record


def notification_html(record):
    esc=lambda value:html.escape(str(value))
    rows=''.join(f'<tr><td style="padding:10px;border-bottom:1px solid #e2e8f0">{esc(i.code)} · {esc(i.description)}</td><td style="padding:10px;text-align:right">{i.quantity}</td></tr>' for i in record.items)
    return f'''<div style="font-family:Arial,sans-serif;background:#f1f5f9;padding:24px"><div style="max-width:640px;margin:auto;background:white;padding:28px;border-radius:18px"><p style="color:#2563eb">Pacas Holl · {record.numero}</p><h2>Solicitud de mercadería de {esc(record.destination.name)}</h2><p>Solicita a <strong>{esc(record.source.name)}</strong> para recibir en <strong>{esc(record.destination.name)}</strong>.</p><p>{record.fecha:%d/%m/%Y} · Usuario: {esc(record.requester_name)}<br>Vendedor: {esc(record.seller_name)}</p><table style="width:100%;border-collapse:collapse"><tr><th align="left">Producto</th><th>Cantidad</th></tr>{rows}</table><p>{esc(record.observation)}</p><p style="margin-top:28px"><a style="background:#2563eb;color:white;padding:14px 20px;border-radius:10px;text-decoration:none" href="{esc(record.review_url)}">Revisar y autorizar</a></p><p style="color:#64748b;font-size:12px">Pendiente de autorización. Requiere iniciar sesión como administrador. El saldo se comprueba al autorizar.</p></div></div>'''


def notify(ident):
    """Durable pending flag + explicit retry; no stock or order-state mutations."""
    from ..database import get_session_local
    from datetime import timedelta
    with get_session_local()() as db:
        record=db.query(InventoryRequest).filter_by(id=ident).with_for_update().first()
        if not record or record.email_state=='sent':return
        now=w.local_now_naive()
        if record.email_state=='sending' and record.email_attempted_at and now-record.email_attempted_at<timedelta(minutes=5):return
        record.email_state='sending';record.email_attempted_at=now;db.commit()
        try:
            config=db.query(w.EmailConfig).first()
            recipients=[r.email for r in db.query(w.NotificationRecipient).filter_by(active=True).all() if (r.email or '').strip()]
            if not config or not config.active:raise ValueError('Correo emisor desactivado o no configurado')
            if not recipients:raise ValueError('No hay destinatarios de correo activos configurados')
            error=w._send_html_email(subject=f'{record.numero} · Solicitud de mercadería de {record.destination.name}',html_body=notification_html(record),recipients=recipients,sender_email=config.sender_email,sender_name=config.sender_name)
            if error:raise ValueError(error)
            record.email_state='sent';record.email_error='';record.notified_at=w.local_now_naive()
        except Exception as exc:
            record.email_state='failed';record.email_error=(str(exc) if isinstance(exc,ValueError) else 'No se pudo enviar el correo. Reintenta desde el panel.')[:240]
        db.commit()
