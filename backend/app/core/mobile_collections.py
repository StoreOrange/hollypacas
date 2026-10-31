"""Holl mobile collections: existing receivables, scoped ownership and atomic payments."""
import json
import hashlib
import hmac
import io
import re
import uuid
import warnings
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from PIL import Image, ImageOps
from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import joinedload, selectinload
from ..models.sales import MobileCollectionPayment, MobileCollectionBasis
from ..routers import web as w

CENT = Decimal('.01')
MAX_UPLOAD = 8 * 1024 * 1024
METHODS = {'deposito': 'Depósito bancario', 'transferencia': 'Transferencia bancaria', 'efectivo': 'Efectivo'}


def is_admin(user):
    return any(r.name == 'administrador' for r in user.roles or []) and not w._restricted_motorizado(user)


def enforce(user):
    if not w._is_pacasholl_company():
        raise HTTPException(404, 'No disponible')
    if w._restricted_motorizado(user) or not (is_admin(user) or user.vendedor_id):
        raise HTTPException(403, 'Tu usuario necesita estar vinculado a un vendedor. Solicita al administrador revisar esa asignación.')


def invoice_query(db, user):
    enforce(user)
    query = db.query(w.VentaFactura).join(w.Bodega, w.Bodega.id == w.VentaFactura.bodega_id).join(w.Branch, w.Branch.id == w.Bodega.branch_id)
    query = query.filter(func.lower(w.Branch.code).in_(w._allowed_branch_codes(db)))
    if not is_admin(user):
        query = query.filter(w.VentaFactura.vendedor_id == user.vendedor_id)
    return query


def invoice(db, user, invoice_id, lock=False):
    q = invoice_query(db, user).filter(w.VentaFactura.id == invoice_id)
    if lock:
        # Lock only the invoice, not the outer relationships loaded for display.
        q = q.with_for_update(of=w.VentaFactura).populate_existing()
    row = q.first()
    if not row:
        raise HTTPException(404, 'Factura no encontrada en tu cartera')
    return row


def amount(value):
    try:
        number = Decimal(str(value))
    except (ValueError, InvalidOperation):
        raise ValueError('Escribe un monto válido, por ejemplo 1500.00.')
    if not number.is_finite() or number <= 0 or number > Decimal('9999999999.99') or number != number.quantize(CENT):
        raise ValueError('El monto debe ser positivo y tener hasta dos decimales.')
    return number


def deferred_initial_payments(factura):
    basis = factura.mobile_collection_basis
    if basis:
        return set(json.loads(basis.ignored_payment_ids))
    # Holl allows a fully-tendered sale to be marked PENDIENTE explicitly.
    # Those tender rows describe the original payment form, not received money.
    # Partial real payments are retained; paid invoices are never reopened here.
    currency = factura.moneda or 'CS'
    total = Decimal(str((factura.total_usd if currency == 'USD' else factura.total_cs) or 0))
    tendered = sum((Decimal(str((p.monto_usd if currency == 'USD' else p.monto_cs) or 0)) for p in factura.pagos), Decimal(0))
    if factura.estado_cobranza == 'PENDIENTE' and total > 0 and tendered >= total:
        return {p.id for p in factura.pagos}
    return set()


def balance(factura):
    ab_usd, ab_cs = w._cobranza_abonos_totals(list(factura.abonos or []))
    ignored = deferred_initial_payments(factura)
    initial = [p for p in factura.pagos if p.id not in ignored]
    paid_cs = ab_cs + sum((Decimal(str(p.monto_cs or 0)) for p in initial), Decimal(0))
    paid_usd = ab_usd + sum((Decimal(str(p.monto_usd or 0)) for p in initial), Decimal(0))
    currency = factura.moneda or 'CS'
    total = Decimal(str((factura.total_usd if currency == 'USD' else factura.total_cs) or 0))
    paid = paid_usd if currency == 'USD' else paid_cs
    due = max(total - paid, Decimal(0)).quantize(CENT)
    # Use the original invoice's proportions for equivalent portfolio figures;
    # the amount to collect always uses the invoice's actual native balance.
    total_cs, total_usd = Decimal(str(factura.total_cs or 0)), Decimal(str(factura.total_usd or 0))
    ratio = due / total if total else Decimal(0)
    return dict(currency=currency, total=total, paid=paid, due=due,
                due_cs=(total_cs * ratio).quantize(CENT), due_usd=(total_usd * ratio).quantize(CENT), deferred_ids=ignored)


def loaded(query):
    return query.options(selectinload(w.VentaFactura.abonos), selectinload(w.VentaFactura.pagos), selectinload(w.VentaFactura.mobile_collection_basis),
                         joinedload(w.VentaFactura.cliente), joinedload(w.VentaFactura.vendedor), joinedload(w.VentaFactura.bodega))


def portfolio(db, user):
    result = []
    for f in loaded(invoice_query(db, user)).filter(w.VentaFactura.estado != 'ANULADA').order_by(w.VentaFactura.fecha.desc(), w.VentaFactura.id.desc()).all():
        values = balance(f)
        result.append(dict(invoice=f, **values))
    return result


def form_token(user, invoice_id, operation=None):
    operation = operation or uuid.uuid4().hex
    message = f'{user.id}:{invoice_id}:{operation}'.encode()
    signature = hmac.new(w.SECRET_KEY.encode(), message, hashlib.sha256).hexdigest()
    return operation + '.' + signature


def operation_key(user, invoice_id, token):
    if not re.fullmatch(r'[a-f0-9]{32}\.[a-f0-9]{64}', str(token or '')):
        raise ValueError('Formulario vencido o inválido. Abre nuevamente la factura.')
    operation = token.split('.')[0]
    if not hmac.compare_digest(form_token(user, invoice_id, operation), token):
        raise ValueError('Formulario inválido. Abre nuevamente la factura.')
    return operation


def clean_image(raw):
    if not raw or len(raw) > MAX_UPLOAD:
        raise ValueError('Adjunta una imagen de hasta 8 MB.')
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error', Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as source:
                if source.format not in {'JPEG', 'PNG', 'WEBP'} or source.width * source.height > 25000000:
                    raise ValueError('Usa una foto JPG, PNG o WebP de hasta 25 megapíxeles.')
                source.load()
                image = ImageOps.exif_transpose(source).convert('RGB')
                image.thumbnail((2200, 2200))
                output = io.BytesIO()
                image.save(output, format='JPEG', quality=88)
                return output.getvalue()
    except (OSError, SyntaxError, Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise ValueError('La imagen no es válida. Adjunta una foto JPG, PNG o WebP.')


def register(db, user, invoice_id, values, image=b''):
    enforce(user)
    key = operation_key(user, invoice_id, values.get('token'))
    f = invoice(db, user, invoice_id, lock=True)
    existing = db.query(MobileCollectionPayment).filter_by(operation_key=key).first()
    if existing:
        if existing.user_id != user.id or existing.abono.factura_id != f.id:
            raise ValueError('La solicitud no corresponde a esta factura.')
        return existing
    if f.estado == 'ANULADA':
        raise ValueError('La factura está anulada; no admite pagos.')
    pending = balance(f)
    if not pending['due']:
        raise ValueError('Esta factura ya no tiene saldo pendiente.')
    paid = amount(values.get('amount'))
    currency = values.get('currency', '')
    method = values.get('method', '')
    reference = str(values.get('reference', '')).strip()
    bank = str(values.get('bank', '')).strip()
    note = str(values.get('note', '')).strip()
    if currency not in {'CS', 'USD'} or method not in METHODS:
        raise ValueError('Selecciona moneda y forma de pago válidas.')
    if len(reference) > 100 or len(bank) > 100 or len(note) > 300:
        raise ValueError('Acorta el banco, la referencia o la observación.')
    try:
        paid_date = date.fromisoformat(str(values.get('date', '')))
    except ValueError:
        raise ValueError('Selecciona la fecha del pago.')
    if paid_date > w.local_today() or (f.fecha and paid_date < f.fecha.date()):
        raise ValueError('La fecha debe estar entre la emisión de la factura y hoy.')
    if method != 'efectivo' and (not reference or not bank or not image):
        raise ValueError('Para un depósito o transferencia indica banco, referencia y foto de soporte.')
    if method != 'efectivo' and db.query(MobileCollectionPayment.id).join(w.CobranzaAbono).filter(
        w.CobranzaAbono.factura_id == f.id, func.lower(MobileCollectionPayment.bank) == bank.lower(),
        func.lower(MobileCollectionPayment.reference) == reference.lower()).first():
        raise ValueError('Esta referencia bancaria ya está registrada en la factura. Revisa el historial.')
    support = clean_image(image) if image else None
    rate_row = db.query(w.ExchangeRate).filter(w.ExchangeRate.effective_date <= paid_date).order_by(w.ExchangeRate.effective_date.desc(), w.ExchangeRate.id.desc()).first()
    rate = Decimal(str(rate_row.rate or 0)) if rate_row else Decimal(0)
    if rate <= 0:
        raise ValueError('No hay una tasa de cambio válida para la fecha del pago.')
    paid_usd = paid if currency == 'USD' else (paid / rate).quantize(CENT, rounding=ROUND_HALF_UP)
    paid_cs = paid if currency == 'CS' else (paid * rate).quantize(CENT, rounding=ROUND_HALF_UP)
    native = paid_usd if pending['currency'] == 'USD' else paid_cs
    raw_native = paid if currency == pending['currency'] else (paid / rate if pending['currency'] == 'USD' else paid * rate)
    if native <= 0 or raw_native > pending['due']:
        raise ValueError(f"El pago excede el saldo de {pending['currency']} {pending['due']:,.2f} o es demasiado pequeño.")
    # Serialize the branch receipt sequence across different invoices, including desktop collections.
    warehouse = db.query(w.Bodega).filter(w.Bodega.id == f.bodega_id).with_for_update().one()
    seq = (db.query(func.max(w.CobranzaAbono.secuencia)).filter(w.CobranzaAbono.bodega_id == warehouse.id).scalar() or 0) + 1
    prefix, series = w._branch_cobranza_series_meta(warehouse.branch.code.lower())
    abono = w.CobranzaAbono(factura_id=f.id, branch_id=warehouse.branch_id, bodega_id=warehouse.id,
        secuencia=seq, numero=f'{prefix}-{series}-{seq:04d}', fecha=paid_date, tipo_mov='ABONO',
        moneda=currency, tasa_cambio=rate, monto_usd=paid_usd, monto_cs=paid_cs, afecta_caja=False,
        observacion=note or f'{METHODS[method]} · {reference}'[:300], usuario_registro=user.full_name or user.email)
    if pending['deferred_ids'] and not f.mobile_collection_basis:
        f.mobile_collection_basis = MobileCollectionBasis(ignored_payment_ids=json.dumps(sorted(pending['deferred_ids'])), user_id=user.id)
    db.add(abono)
    db.flush()
    payment = MobileCollectionPayment(operation_key=key, abono_id=abono.id, user_id=user.id,
        vendedor_id=f.vendedor_id, method=method, reference=reference, bank=bank, support=support)
    db.add(payment)
    f.estado_cobranza = 'PAGADA' if native == pending['due'] else 'PENDIENTE'
    db.flush()
    return payment
