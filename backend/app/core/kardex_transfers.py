"""Read-only reconstruction of missing variant metadata on legacy transfer receipts.

The destination receipt must explicitly name its source document and reconcile
its product quantities. Never infer an inventory adjustment or write stock.
"""
import re
from collections import defaultdict
from decimal import Decimal
from sqlalchemy.orm import selectinload
from ..models.inventory import (IngresoInventario, IngresoItem, EgresoInventario,
    EgresoItem, ShoeProductVariant, Bodega)


def legacy_variant_receipts(db, variant_id, end_date, allowed_bodega_ids=None, bodega_id=None):
    variant = db.get(ShoeProductVariant, variant_id)
    if not variant:
        return []
    query = (db.query(IngresoInventario).join(IngresoItem)
        .filter(IngresoItem.producto_id == variant.producto_id,
                IngresoItem.variante_id.is_(None), IngresoInventario.fecha <= end_date))
    if allowed_bodega_ids is not None:
        query = query.filter(IngresoInventario.bodega_id.in_(allowed_bodega_ids))
    if bodega_id:
        query = query.filter(IngresoInventario.bodega_id == bodega_id)
    receipts = query.options(selectinload(IngresoInventario.items)).distinct().all()
    linked = defaultdict(list)
    for receipt in receipts:
        references = re.findall(r'\bEgreso\s*#\s*(\d+)\b', receipt.observacion or '', re.I)
        if len(references) == 1:
            linked[int(references[0])].append(receipt)
    if not linked:
        return []
    sources = (db.query(EgresoInventario).filter(EgresoInventario.id.in_(linked))
        .options(selectinload(EgresoInventario.items)).all())
    result = []
    for source in sources:
        if len(linked[source.id]) != 1 or not source.bodega_destino_id:
            continue
        receipt = linked[source.id][0]
        if receipt.bodega_id != source.bodega_destino_id or receipt.fecha != source.fecha:
            continue
        if 'traslado' not in (source.tipo.nombre or '').lower() or 'traslado' not in (receipt.tipo.nombre or '').lower():
            continue
        outgoing = [i for i in source.items if i.producto_id == variant.producto_id]
        incoming = [i for i in receipt.items if i.producto_id == variant.producto_id]
        # A complete product-level match is required before assigning any missing
        # variant quantity. Explicitly recorded variants are never counted twice.
        if not outgoing or any(not i.variante_id or i.cantidad <= 0 for i in outgoing):
            continue
        expected, recorded = defaultdict(Decimal), defaultdict(Decimal)
        for item in outgoing:
            expected[item.variante_id] += item.cantidad
        for item in incoming:
            recorded[item.variante_id] += item.cantidad
        if sum(expected.values()) != sum(recorded.values()) or any(q < 0 for q in recorded.values()):
            continue
        if any(q > expected[vid] for vid, q in recorded.items() if vid is not None):
            continue
        missing = expected[variant_id] - recorded[variant_id]
        if missing <= 0 or missing > recorded[None]:
            continue
        lines = [i for i in outgoing if i.variante_id == variant_id]
        qty = expected[variant_id]
        result.append(dict(receipt=receipt, variant=variant, quantity=missing,
            source_id=source.id,
            cost_cs=sum(i.cantidad * (i.costo_unitario_cs or 0) for i in lines) / qty,
            cost_usd=sum(i.cantidad * (i.costo_unitario_usd or 0) for i in lines) / qty))
    return result


def enrich_movement_documents(db, movements):
    """Attach direct PDFs and verified transfer warehouse names without writes."""
    receipts = {r.id: r for r in db.query(IngresoInventario).filter(
        IngresoInventario.id.in_({m['source_id'] for m in movements if m['source_kind'] == 'ingreso'})).all()}
    references = {}
    for receipt in receipts.values():
        ids = re.findall(r'\bEgreso\s*#\s*(\d+)\b', receipt.observacion or '', re.I)
        if len(ids) == 1:
            references[receipt.id] = int(ids[0])
    source_ids = set(references.values()) | {m['source_id'] for m in movements if m['source_kind'] == 'egreso'}
    sources = {e.id: e for e in db.query(EgresoInventario).filter(EgresoInventario.id.in_(source_ids)).all()}
    warehouses = {b.id: b.name for b in db.query(Bodega).all()}
    paths = {'ingreso': '/inventory/ingresos', 'egreso': '/inventory/egresos', 'venta': '/sales'}
    for movement in movements:
        kind, ident = movement['source_kind'], movement['source_id']
        movement['pdf_url'] = f"{paths[kind]}/{ident}/pdf"
        source = sources.get(ident if kind == 'egreso' else references.get(ident)) if kind != 'venta' else None
        if not source or not source.bodega_destino_id or 'traslado' not in (source.tipo.nombre or '').lower():
            continue
        if kind == 'ingreso' and receipts[ident].bodega_id != source.bodega_destino_id:
            continue
        origin = warehouses.get(source.bodega_id, 'Origen no identificado')
        destination = warehouses.get(source.bodega_destino_id, 'Destino no identificado')
        movement['transfer_route'] = f'{origin} → {destination}'
        movement['concepto'] = f"{movement['concepto']} · {origin} → {destination}"
