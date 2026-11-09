"""Document-date ledger with registration order within each accounting day."""
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo


def prepare_chronology(movements):
    local = ZoneInfo('America/Managua')
    for row in movements:
        stamp = row.get('registro_utc')
        if stamp is not None:
            # POS explicitly writes local_now_naive(); inventory documents use
            # the database UTC default. Applying UTC to both shifts sales by 6h.
            origin = local if row.get('source_kind') == 'venta' else timezone.utc
            stamp = stamp.replace(tzinfo=origin) if stamp.tzinfo is None else stamp
            row['registrado_en'] = stamp.astimezone(local).replace(tzinfo=None)
        else:
            row['registrado_en'] = None
    movements.sort(key=lambda row: (
        row['fecha'],
        row['registrado_en'] or datetime.combine(row['fecha'], time.min),
        # Simultaneous transfers: show dispatch before destination receipt.
        {'Egreso': 0, 'Ingreso': 1, 'Venta': 2}.get(row.get('tipo_base'), 9),
        int(row.get('source_id') or 0), int(row.get('bodega_id') or 0),
        int(row.get('producto_id') or 0),
    ))
