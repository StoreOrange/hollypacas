"""Document-date ledger with registration order within each accounting day."""
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo


def prepare_chronology(movements):
    local = ZoneInfo('America/Managua')
    for row in movements:
        stamp = row.get('registro_utc')
        if stamp is not None:
            stamp = stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp
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
