from typing import Optional

import csv
import json
import os
import re
import smtplib
import subprocess
import tempfile
from email.message import EmailMessage

import io
from pathlib import Path
from dotenv import dotenv_values
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font
from reportlab.lib import colors

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from jose import JWTError, jwt
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from ..config import settings
from ..core.deps import get_db, require_admin
from ..core.security import (
    ALGORITHM,
    SECRET_KEY,
    create_access_token,
    hash_password,
    verify_password,
)
from ..core.utils import local_now, local_now_naive, local_today
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from ..models.inventory import (
    Bodega,
    EgresoInventario,
    EgresoItem,
    EgresoTipo,
    ExchangeRate,
    IngresoInventario,
    IngresoItem,
    IngresoTipo,
    Linea,
    Producto,
    ProductoCombo,
    Proveedor,
    SaldoProducto,
    Segmento,
)
from ..models.sales import (
    Banco,
    CajaDiaria,
    CierreCaja,
    Cliente,
    CobranzaAbono,
    CuentaBancaria,
    CuentaContable,
    DepositoCliente,
    EmailConfig,
    FormaPago,
    NotificationRecipient,
    PosPrintSetting,
    ReciboCaja,
    ReciboMotivo,
    ReciboRubro,
    ReversionToken,
    Vendedor,
    VendedorBodega,
    VentaFactura,
    VentaItem,
    VentaPago,
)
from ..models.user import Branch, Permission, Role, User

router = APIRouter()


def to_decimal(value: Optional[float]) -> Decimal:
    return Decimal(str(value or 0))


def _get_user_from_cookie(request: Request, db: Session) -> Optional[User]:
    token = request.cookies.get("access_token")
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None
    email = payload.get("sub")
    if not email:
        return None
    return db.query(User).filter(func.lower(User.email) == email.lower()).first()


def _permission_names(user: User) -> set[str]:
    return {perm.name for perm in (user.permissions or [])}


def _has_permission(user: User, perm: str) -> bool:
    if any(role.name == "administrador" for role in user.roles or []):
        return True
    return perm in _permission_names(user)


def _set_request_permissions(request: Request, user: User) -> None:
    perm_names = _permission_names(user)
    request.state.permission_names = perm_names
    request.state.has_permissions = bool(perm_names)


def _enforce_permission(request: Request, user: User, perm: str) -> None:
    if not request.state.has_permissions:
        return
    if not _has_permission(user, perm):
        raise HTTPException(status_code=403, detail="Acceso denegado")


PERMISSION_GROUPS = [
    {
        "title": "Menus principales (visibilidad)",
        "items": [
            {"name": "menu.home", "label": "Panel principal"},
            {"name": "menu.sales", "label": "Ventas"},
            {"name": "menu.sales.caliente", "label": "Ventas en caliente"},
            {"name": "menu.inventory", "label": "Inventarios"},
            {"name": "menu.inventory.caliente", "label": "Inventario en caliente"},
            {"name": "menu.finance", "label": "Finanzas"},
            {"name": "menu.reports", "label": "Informes"},
            {"name": "menu.data", "label": "Datos / catalogos"},
        ],
    },
    {
        "title": "Ventas (visibilidad sub-menu)",
        "items": [
            {"name": "menu.sales.utilitario", "label": "Utilitario de ventas"},
            {"name": "menu.sales.cobranza", "label": "Gestion de cobranza"},
            {"name": "menu.sales.roc", "label": "Recibos de caja"},
            {"name": "menu.sales.depositos", "label": "Registro de depositos"},
            {"name": "menu.sales.cierre", "label": "Cierre de caja"},
        ],
    },
    {
        "title": "Inventarios (visibilidad sub-menu)",
        "items": [
            {"name": "menu.inventory.ingresos", "label": "Ingresos de inventario"},
            {"name": "menu.inventory.egresos", "label": "Egresos de inventario"},
        ],
    },
    {
        "title": "Accesos (acciones)",
        "items": [
            {"name": "access.sales", "label": "Acceso a ventas"},
            {"name": "access.sales.registrar", "label": "Registrar facturas"},
            {"name": "access.sales.pagos", "label": "Aplicar pagos"},
            {"name": "access.sales.utilitario", "label": "Utilitario de ventas"},
            {"name": "access.sales.cobranza", "label": "Cobranza / abonos"},
            {"name": "access.sales.roc", "label": "Recibos de caja"},
            {"name": "access.sales.depositos", "label": "Depositos bancarios"},
            {"name": "access.sales.cierre", "label": "Cierre de caja"},
            {"name": "access.sales.reversion", "label": "Reversion de facturas"},
            {"name": "access.inventory", "label": "Acceso a inventarios"},
            {"name": "access.inventory.caliente", "label": "Inventario en caliente"},
            {"name": "access.inventory.ingresos", "label": "Ingresos de inventario"},
            {"name": "access.inventory.egresos", "label": "Egresos de inventario"},
            {"name": "access.inventory.productos", "label": "Crear/editar productos"},
            {"name": "access.finance", "label": "Acceso a finanzas"},
            {"name": "access.reports", "label": "Acceso a informes"},
            {"name": "access.data", "label": "Acceso a datos"},
            {"name": "access.data.permissions", "label": "Gestion de permisos"},
            {"name": "access.data.users", "label": "Usuarios"},
            {"name": "access.data.roles", "label": "Roles"},
            {"name": "access.data.catalogs", "label": "Catalogos"},
        ],
    },
]


def _permission_catalog_names() -> set[str]:
    names: set[str] = set()
    for group in PERMISSION_GROUPS:
        for item in group["items"]:
            names.add(item["name"])
    return names


def _require_admin_web(
    request: Request, db: Session = Depends(get_db)
) -> User:
    user = _get_user_from_cookie(request, db)
    if not user:
        raise HTTPException(status_code=302, headers={"Location": "/login"})
    if user.is_active is False:
        raise HTTPException(status_code=403, detail="Acceso denegado")
    _set_request_permissions(request, user)
    return user


def _require_user_web(
    request: Request, db: Session = Depends(get_db)
) -> User:
    user = _get_user_from_cookie(request, db)
    if not user:
        raise HTTPException(status_code=302, headers={"Location": "/login"})
    if user.is_active is False:
        raise HTTPException(status_code=403, detail="Acceso denegado")
    _set_request_permissions(request, user)
    return user


def _resolve_branch_bodega(db: Session, user: User) -> tuple[Optional[Branch], Optional[Bodega]]:
    user_branches = list(user.branches or [])
    branch = None
    if user.default_branch_id:
        branch = db.query(Branch).filter(Branch.id == user.default_branch_id).first()
    if not branch:
        branch = next((b for b in user_branches if b.code == "central"), None)
    if not branch and user_branches:
        branch = user_branches[0]
    bodega = None
    if branch:
        if user.default_bodega_id:
            bodega = (
                db.query(Bodega)
                .filter(Bodega.id == user.default_bodega_id, Bodega.activo.is_(True))
                .first()
            )
            if bodega and bodega.branch_id != branch.id:
                bodega = None
        if not bodega:
            bodega = (
                db.query(Bodega)
                .filter(Bodega.branch_id == branch.id, Bodega.activo.is_(True))
                .order_by(Bodega.id)
                .first()
            )
    return branch, bodega


def _vendedores_for_bodega(db: Session, bodega: Optional[Bodega]) -> list[Vendedor]:
    base = db.query(Vendedor).filter(Vendedor.activo.is_(True))
    if not bodega:
        return base.order_by(Vendedor.nombre).all()
    assigned = (
        base.join(VendedorBodega, VendedorBodega.vendedor_id == Vendedor.id)
        .filter(VendedorBodega.bodega_id == bodega.id)
        .order_by(Vendedor.nombre)
        .all()
    )
    if assigned:
        return assigned
    return base.order_by(Vendedor.nombre).all()


def _default_vendedor_id(db: Session, bodega: Optional[Bodega]) -> Optional[int]:
    if not bodega:
        return None
    row = (
        db.query(VendedorBodega)
        .filter(VendedorBodega.bodega_id == bodega.id, VendedorBodega.is_default.is_(True))
        .first()
    )
    return row.vendedor_id if row else None


def _get_sumatra_path(config_path: Optional[str] = None) -> Optional[Path]:
    env_path = os.getenv("SUMATRA_PATH")
    candidates = [
        config_path,
        env_path,
        r"C:\Program Files\SumatraPDF\SumatraPDF.exe",
        r"C:\Program Files (x86)\SumatraPDF\SumatraPDF.exe",
        r"C:\Users\USER\AppData\Local\SumatraPDF\SumatraPDF.exe",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists():
            return path
    return None


def _balances_by_bodega(
    db: Session,
    bodega_ids: list[int],
    product_ids: list[int],
) -> dict[tuple[int, int], Decimal]:
    if not bodega_ids or not product_ids:
        return {}
    ingreso_rows = (
        db.query(IngresoItem.producto_id, IngresoInventario.bodega_id, func.sum(IngresoItem.cantidad))
        .join(IngresoInventario, IngresoInventario.id == IngresoItem.ingreso_id)
        .filter(IngresoInventario.bodega_id.in_(bodega_ids))
        .filter(IngresoItem.producto_id.in_(product_ids))
        .group_by(IngresoItem.producto_id, IngresoInventario.bodega_id)
        .all()
    )
    egreso_rows = (
        db.query(EgresoItem.producto_id, EgresoInventario.bodega_id, func.sum(EgresoItem.cantidad))
        .join(EgresoInventario, EgresoInventario.id == EgresoItem.egreso_id)
        .filter(EgresoInventario.bodega_id.in_(bodega_ids))
        .filter(EgresoItem.producto_id.in_(product_ids))
        .group_by(EgresoItem.producto_id, EgresoInventario.bodega_id)
        .all()
    )
    venta_rows = (
        db.query(VentaItem.producto_id, VentaFactura.bodega_id, func.sum(VentaItem.cantidad))
        .join(VentaFactura, VentaFactura.id == VentaItem.factura_id)
        .filter(VentaFactura.bodega_id.in_(bodega_ids))
        .filter(VentaItem.producto_id.in_(product_ids))
        .filter(VentaFactura.estado != "ANULADA")
        .group_by(VentaItem.producto_id, VentaFactura.bodega_id)
        .all()
    )
    balances: dict[tuple[int, int], Decimal] = {}
    for producto_id, bodega_id, qty in ingreso_rows:
        balances[(producto_id, bodega_id)] = Decimal(str(qty or 0))
    for producto_id, bodega_id, qty in egreso_rows:
        balances[(producto_id, bodega_id)] = balances.get((producto_id, bodega_id), Decimal("0")) - Decimal(str(qty or 0))
    for producto_id, bodega_id, qty in venta_rows:
        balances[(producto_id, bodega_id)] = balances.get((producto_id, bodega_id), Decimal("0")) - Decimal(str(qty or 0))
    return balances


def _build_pos_ticket_pdf_bytes(factura: VentaFactura) -> bytes:
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas

    def wrap_text(text: str, max_chars: int) -> list[str]:
        if not text:
            return [""]
        words = text.split()
        lines: list[str] = []
        current = ""
        for word in words:
            candidate = f"{current} {word}".strip()
            if len(candidate) > max_chars:
                if current:
                    lines.append(current)
                current = word
            else:
                current = candidate
        if current:
            lines.append(current)
        return lines or [text]

    def format_qty(value: float) -> str:
        return f"{value:.2f}".rstrip("0").rstrip(".")

    def format_amount(value: float) -> str:
        return f"{value:,.2f}"

    def extract_weight_lbs(text: str) -> float:
        if not text:
            return 0.0
        match = re.search(r"\b(\d+(?:\.\d+)?)\s*(lbs)\b", text.lower())
        if not match:
            return 0.0
        try:
            return float(match.group(1))
        except ValueError:
            return 0.0

    branch = factura.bodega.branch if factura.bodega else None
    company_name = (branch.company_name if branch and branch.company_name else "Pacas Hollywood").strip()
    ruc = branch.ruc if branch and branch.ruc else "-"
    telefono = branch.telefono if branch and branch.telefono else "-"
    direccion = branch.direccion if branch and branch.direccion else "-"
    sucursal = branch.name if branch and branch.name else "-"

    cliente = factura.cliente.nombre if factura.cliente else "Consumidor final"
    cliente_id = factura.cliente.identificacion if factura.cliente and factura.cliente.identificacion else "-"
    vendedor = factura.vendedor.nombre if factura.vendedor else "-"

    fecha_base = factura.created_at or factura.fecha
    fecha_str = ""
    hora_str = ""
    if fecha_base:
        try:
            fecha_str = fecha_base.strftime("%d/%m/%Y")
            hora_str = fecha_base.strftime("%H:%M")
        except AttributeError:
            fecha_str = str(fecha_base)

    moneda = factura.moneda or "CS"
    currency_label = "C$" if moneda == "CS" else "$"
    total_amount = float(factura.total_cs or 0) if moneda == "CS" else float(factura.total_usd or 0)
    subtotal_amount = total_amount

    pagos = factura.pagos or []
    total_paid = sum(
        float(pago.monto_cs or 0) if moneda == "CS" else float(pago.monto_usd or 0)
        for pago in pagos
    )
    saldo = total_paid - total_amount

    lines: list[tuple[str, str, bool, int]] = []

    def add_line(text: str, align: str = "left", bold: bool = False, size: int = 8):
        lines.append((text, align, bold, size))

    add_line(company_name.upper(), "center", True, 10)
    add_line(f"RUC: {ruc}", "center")
    add_line(f"Tel: {telefono}", "center")
    direccion_lines = wrap_text(direccion, 32)[:2]
    for line in direccion_lines:
        add_line(line, "center")
    add_line(f"Sucursal: {sucursal}", "center", True, 9)
    add_line("-" * 32, "center")
    add_line(f"Factura: {factura.numero}", "left", True)
    add_line(f"Fecha: {fecha_str} {hora_str}".strip())
    add_line(f"Cliente: {cliente}")
    add_line(f"Identificacion R/C: {cliente_id}")
    add_line(f"Vendedor: {vendedor}")
    add_line("-" * 32, "center")

    max_desc = 32
    total_bultos = 0.0
    total_lbs = 0.0
    for item in factura.items:
        codigo = item.producto.cod_producto if item.producto else "-"
        descripcion = item.producto.descripcion if item.producto else "-"
        combo_label = ""
        if item.combo_role == "gift":
            combo_label = " [REGALO]"
        elif item.combo_role == "parent":
            combo_label = " [OFERTA]"
        qty = float(item.cantidad or 0)
        price = (
            float(item.precio_unitario_cs or 0)
            if moneda == "CS"
            else float(item.precio_unitario_usd or 0)
        )
        subtotal = (
            float(item.subtotal_cs or 0)
            if moneda == "CS"
            else float(item.subtotal_usd or 0)
        )
        total_bultos += qty
        lbs_per_unit = extract_weight_lbs(descripcion)
        if lbs_per_unit:
            total_lbs += lbs_per_unit * qty
        add_line(f"Codigo: {codigo}{combo_label}", "left", True, 9)
        for part in wrap_text(descripcion, max_desc):
            add_line(part, "left", False, 9)
        add_line(
            f"Cant: {format_qty(qty)}  Precio: {currency_label} {format_amount(price)}",
            "left",
            False,
            9,
        )
        add_line(
            f"Desc: {currency_label} 0.00  Subtotal: {currency_label} {format_amount(subtotal)}",
            "left",
            False,
            9,
        )
        add_line("-" * 32, "center")

    add_line(f"Total bultos: {format_qty(total_bultos)}", "left", True, 9)
    add_line(f"Total libras: {format_qty(total_lbs)}", "left", True, 9)
    add_line(f"Subtotal: {currency_label} {format_amount(subtotal_amount)}", "right", True, 9)
    add_line(f"Descuentos: {currency_label} 0.00", "right", False, 9)
    add_line(f"Total: {currency_label} {format_amount(total_amount)}", "right", True, 10)

    if pagos:
        add_line("-" * 32, "center")
        add_line("Pagos aplicados", "left", True, 9)
        for pago in pagos:
            forma = pago.forma_pago.nombre if pago.forma_pago else "Pago"
            banco = pago.banco.nombre if pago.banco else ""
            label = f"{forma} {banco}".strip()
            monto = (
                float(pago.monto_cs or 0)
                if moneda == "CS"
                else float(pago.monto_usd or 0)
            )
            add_line(f"{label}: {currency_label} {format_amount(monto)}", "left", False, 9)

    if saldo >= 0:
        add_line(f"Vuelto: {currency_label} {format_amount(saldo)}", "left", True, 9)
    else:
        add_line(f"Saldo: {currency_label} {format_amount(abs(saldo))}", "left", True, 9)

    add_line("")
    add_line("Gracias por su compra", "center", True, 9)
    add_line("Revise su mercaderia antes de salir.", "center", False, 9)
    add_line("No se aceptan cambios ni devoluciones", "center", False, 9)
    add_line("de mercaderia ni de dinero.", "center", False, 9)

    content_width = 70 * mm
    width = 80 * mm
    margin = (width - content_width) / 2
    top_margin = 6 * mm
    bottom_margin = 6 * mm
    logo_path = Path(__file__).resolve().parents[1] / "static" / "logopos.png"
    logo_height = 52 * mm if logo_path.exists() else 0
    logo_spacing = 3 * mm if logo_height else 0

    def line_gap(size: int) -> float:
        return size + 4

    total_height = top_margin + bottom_margin + logo_height + logo_spacing
    total_height += sum(line_gap(size) for _, _, _, size in lines)
    total_height = max(total_height, 120 * mm)

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=(width, total_height))
    y = total_height - top_margin

    if logo_height:
        logo_width = 75 * mm
        pdf.drawImage(
            str(logo_path),
            (width - logo_width) / 2,
            y - logo_height,
            width=logo_width,
            height=logo_height,
            preserveAspectRatio=True,
            mask="auto",
        )
        y -= logo_height + logo_spacing

    for text, align, bold, size in lines:
        pdf.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        if align == "center":
            pdf.drawCentredString(width / 2, y, text)
        elif align == "right":
            pdf.drawRightString(width - margin, y, text)
        else:
            pdf.drawString(margin, y, text)
        y -= line_gap(size)

    pdf.showPage()
    pdf.save()
    return buffer.getvalue()


def _print_pos_ticket(
    factura: VentaFactura,
    printer_name: str,
    copies: int,
    sumatra_override: Optional[str] = None,
) -> None:
    sumatra_path = _get_sumatra_path(sumatra_override)
    if not sumatra_path:
        return
    pdf_bytes = _build_pos_ticket_pdf_bytes(factura)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
        tmp_file.write(pdf_bytes)
        tmp_path = tmp_file.name
    try:
        for _ in range(max(copies, 1)):
            subprocess.run(
                [
                    str(sumatra_path),
                    "-print-to",
                    printer_name,
                    "-print-settings",
                    "noscale",
                    "-silent",
                    tmp_path,
                ],
                check=False,
            )
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except OSError:
            pass


def _build_roc_ticket_pdf_bytes(recibo: ReciboCaja) -> bytes:
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas

    def wrap_text(text: str, max_chars: int) -> list[str]:
        if not text:
            return [""]
        words = text.split()
        lines: list[str] = []
        current = ""
        for word in words:
            candidate = f"{current} {word}".strip()
            if len(candidate) > max_chars:
                if current:
                    lines.append(current)
                current = word
            else:
                current = candidate
        if current:
            lines.append(current)
        return lines or [text]

    def format_amount(value: float) -> str:
        return f"{value:,.2f}"

    branch = recibo.branch
    company_name = (branch.company_name if branch and branch.company_name else "Pacas Hollywood").strip()
    ruc = branch.ruc if branch and branch.ruc else "-"
    telefono = branch.telefono if branch and branch.telefono else "-"
    direccion = branch.direccion if branch and branch.direccion else "-"
    sucursal = branch.name if branch and branch.name else "-"
    bodega_name = recibo.bodega.name if recibo.bodega else "-"

    fecha_base = recibo.created_at or recibo.fecha
    fecha_str = ""
    hora_str = ""
    if fecha_base:
        try:
            fecha_str = fecha_base.strftime("%d/%m/%Y")
            hora_str = fecha_base.strftime("%H:%M")
        except AttributeError:
            fecha_str = str(fecha_base)

    moneda = recibo.moneda or "CS"
    currency_label = "C$" if moneda == "CS" else "$"
    monto_total = float(recibo.monto_cs or 0) if moneda == "CS" else float(recibo.monto_usd or 0)
    monto_cs = float(recibo.monto_cs or 0)

    rubro = recibo.rubro.nombre if recibo.rubro else "-"
    motivo = recibo.motivo.nombre if recibo.motivo else "-"
    descripcion = recibo.descripcion or "-"
    rubro_codigo = recibo.rubro.cuenta.codigo if recibo.rubro and recibo.rubro.cuenta else "9999"
    rubro_cuenta = f"{rubro_codigo} {rubro}"
    caja_cuenta = "1101 Caja"

    lines: list[tuple[str, str, bool, int]] = []

    def add_line(text: str, align: str = "left", bold: bool = False, size: int = 8):
        lines.append((text, align, bold, size))

    add_line(company_name.upper(), "center", True, 10)
    add_line(f"RUC: {ruc}", "center")
    add_line(f"Tel: {telefono}", "center")
    direccion_lines = wrap_text(direccion, 32)[:2]
    for line in direccion_lines:
        add_line(line, "center")
    add_line(f"Sucursal: {sucursal}", "center", True, 9)
    add_line("-" * 32, "center")
    add_line("RECIBO OFICIAL DE CAJA", "center", True, 10)
    add_line("-" * 32, "center")
    add_line(f"No. Recibo: {recibo.numero}", "left", True, 9)
    add_line(f"Fecha: {fecha_str} {hora_str}".strip())
    add_line(f"Bodega: {bodega_name}")
    add_line(f"Tipo: {recibo.tipo}")
    add_line(f"Rubro: {rubro}")
    add_line(f"Motivo: {motivo}")
    add_line(f"Monto: {currency_label} {format_amount(monto_total)}", "left", True, 9)
    add_line(f"Equivalente C$: {format_amount(monto_cs)}")
    add_line("-" * 32, "center")
    add_line("Detalle", "left", True, 9)
    for part in wrap_text(descripcion, 32):
        add_line(part, "left", False, 9)

    add_line("-" * 32, "center")
    add_line("Mini asiento contable", "left", True, 9)
    if recibo.tipo == "EGRESO":
        add_line(f"Debe: {rubro_cuenta}", "left", False, 9)
        add_line(f"{currency_label} {format_amount(monto_total)}", "right", True, 9)
        add_line(f"Haber: {caja_cuenta}", "left", False, 9)
        add_line(f"{currency_label} {format_amount(monto_total)}", "right", True, 9)
    else:
        add_line(f"Debe: {caja_cuenta}", "left", False, 9)
        add_line(f"{currency_label} {format_amount(monto_total)}", "right", True, 9)
        add_line(f"Haber: {rubro_cuenta}", "left", False, 9)
        add_line(f"{currency_label} {format_amount(monto_total)}", "right", True, 9)

    add_line("-" * 32, "center")
    add_line("Realizado por: __________________", "left", False, 9)
    add_line("Recibido por: ___________________", "left", False, 9)
    add_line("Autorizado: _____________________", "left", False, 9)

    content_width = 70 * mm
    width = 80 * mm
    margin = (width - content_width) / 2
    top_margin = 6 * mm
    bottom_margin = 6 * mm
    logo_path = Path(__file__).resolve().parents[1] / "static" / "logopos.png"
    if not logo_path.exists():
        logo_path = Path(__file__).resolve().parents[1] / "static" / "logo_hollywood.png"
    logo_height = 42 * mm if logo_path.exists() else 0
    logo_spacing = 2 * mm if logo_height else 0

    def line_gap(size: int) -> float:
        return size + 4

    total_height = top_margin + bottom_margin + logo_height + logo_spacing
    total_height += sum(line_gap(size) for _, _, _, size in lines)
    total_height = max(total_height, 140 * mm)

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=(width, total_height))
    y = total_height - top_margin

    if logo_height:
        logo_width = 65 * mm
        pdf.drawImage(
            str(logo_path),
            (width - logo_width) / 2,
            y - logo_height,
            width=logo_width,
            height=logo_height,
            preserveAspectRatio=True,
            mask="auto",
        )
        y -= logo_height + logo_spacing

    for text, align, bold, size in lines:
        pdf.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        if align == "center":
            pdf.drawCentredString(width / 2, y, text)
        elif align == "right":
            pdf.drawRightString(width - margin, y, text)
        else:
            pdf.drawString(margin, y, text)
        y -= line_gap(size)

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.read()


def _print_roc_ticket(
    recibo: ReciboCaja,
    printer_name: str,
    copies: int,
    sumatra_override: Optional[str] = None,
) -> None:
    sumatra_path = _get_sumatra_path(sumatra_override)
    if not sumatra_path:
        return
    pdf_bytes = _build_roc_ticket_pdf_bytes(recibo)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
        tmp_file.write(pdf_bytes)
        tmp_path = tmp_file.name
    try:
        for _ in range(max(copies, 1)):
            subprocess.run(
                [
                    str(sumatra_path),
                    "-print-to",
                    printer_name,
                    "-print-settings",
                    "noscale",
                    "-silent",
                    tmp_path,
                ],
                check=False,
            )
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except OSError:
            pass


def _build_cierre_ticket_pdf_bytes(
    cierre: CierreCaja,
    tasa: Decimal,
    resumen: dict,
    total_bultos: Decimal,
) -> bytes:
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas

    def format_amount(value: Decimal) -> str:
        return f"{Decimal(str(value or 0)):,.2f}"

    def wrap_text(text: str, max_chars: int) -> list[str]:
        if not text:
            return [""]
        words = text.split()
        lines: list[str] = []
        current = ""
        for word in words:
            candidate = f"{current} {word}".strip()
            if len(candidate) > max_chars:
                if current:
                    lines.append(current)
                current = word
            else:
                current = candidate
        if current:
            lines.append(current)
        return lines or [text]

    branch = cierre.branch
    company_name = (branch.company_name if branch and branch.company_name else "Pacas Hollywood").strip()
    ruc = branch.ruc if branch and branch.ruc else "-"
    telefono = branch.telefono if branch and branch.telefono else "-"
    direccion = branch.direccion if branch and branch.direccion else "-"
    sucursal = branch.name if branch and branch.name else "-"
    bodega_name = cierre.bodega.name if cierre.bodega else "-"

    fecha_str = cierre.fecha.strftime("%d/%m/%Y") if cierre.fecha else ""

    lines: list[tuple[str, str, bool, int]] = []

    def add_line(text: str, align: str = "left", bold: bool = False, size: int = 9):
        lines.append((text, align, bold, size))

    add_line(company_name.upper(), "center", True, 12)
    add_line(f"RUC: {ruc}", "center")
    add_line(f"Tel: {telefono}", "center")
    direccion_lines = wrap_text(direccion, 32)[:2]
    for line in direccion_lines:
        add_line(line, "center")
    add_line(f"Sucursal: {sucursal}", "center", True, 10)
    add_line("-" * 32, "center")
    add_line("CIERRE OFICIAL DE CAJA", "center", True, 12)
    add_line("-" * 32, "center")
    add_line(f"Fecha: {fecha_str}", "left", True, 10)
    add_line(f"Bodega: {bodega_name}", "left", False, 10)
    add_line("-" * 32, "center")

    add_line("Resumen arqueo (USD)", "left", True, 10)
    add_line(f"Ventas: $ {format_amount(resumen['ventas_usd'])}", "left", False, 10)
    add_line(f"Ingresos: + $ {format_amount(resumen['ingresos_usd'])}", "left", False, 10)
    add_line(f"Egresos: - $ {format_amount(resumen['egresos_usd'])}", "left", False, 10)
    add_line(f"Depositos: - $ {format_amount(resumen['depositos_usd'])}", "left", False, 10)
    add_line(f"Creditos: - $ {format_amount(resumen['creditos_usd'])}", "left", False, 10)
    add_line(f"Total esperado: $ {format_amount(resumen['total_calculado_usd'])}", "left", True, 10)
    add_line("")

    add_line("Efectivo contado", "left", True, 10)
    add_line(f"Total C$: {format_amount(cierre.total_efectivo_cs)}", "left", False, 10)
    add_line(f"Total USD: {format_amount(cierre.total_efectivo_usd)}", "left", False, 10)
    add_line(f"Total USD equiv: {format_amount(cierre.total_efectivo_usd_equiv)}", "left", True, 10)
    add_line("-" * 32, "center")

    add_line(f"Faltante/Sobrante: $ {format_amount(cierre.diferencia_usd)}", "left", True, 10)
    add_line(f"Total bultos vendidos: {format_amount(total_bultos)}", "left", False, 10)
    add_line("")

    try:
        detalle_cs = json.loads(cierre.detalle_cs or "{}")
    except Exception:
        detalle_cs = {}
    try:
        detalle_usd = json.loads(cierre.detalle_usd or "{}")
    except Exception:
        detalle_usd = {}

    add_line("Desglose USD (cantidades)", "left", True, 10)
    usd_items = []
    for denom, qty in detalle_usd.items():
        try:
            usd_items.append((Decimal(str(denom)), qty))
        except Exception:
            usd_items.append((Decimal("0"), qty))
    usd_items = sorted(usd_items, key=lambda item: item[0], reverse=True)
    subtotal_usd_breakdown = Decimal("0")
    for denom, qty in usd_items:
        qty_dec = Decimal(str(qty or 0))
        total = denom * qty_dec
        subtotal_usd_breakdown += total
        add_line(f"$ {denom} x {qty} = $ {format_amount(total)}", "left", False, 9)
    add_line("")
    add_line(f"Total desglose USD: {format_amount(subtotal_usd_breakdown)}", "left", True, 10)
    add_line("")

    add_line("Desglose C$ (cantidades)", "left", True, 10)
    cs_items = []
    for denom, qty in detalle_cs.items():
        try:
            cs_items.append((Decimal(str(denom)), qty))
        except Exception:
            cs_items.append((Decimal("0"), qty))
    cs_items = sorted(cs_items, key=lambda item: item[0], reverse=True)
    subtotal_cs_breakdown = Decimal("0")
    for denom, qty in cs_items:
        qty_dec = Decimal(str(qty or 0))
        total = denom * qty_dec
        subtotal_cs_breakdown += total
        add_line(f"C$ {denom} x {qty} = C$ {format_amount(total)}", "left", False, 9)
    add_line("")
    add_line(f"Total desglose C$: {format_amount(subtotal_cs_breakdown)}", "left", True, 10)
    add_line("")
    add_line(f"Total C$: {format_amount(cierre.total_efectivo_cs)}", "left", True, 10)
    add_line(f"Total USD: {format_amount(cierre.total_efectivo_usd)}", "left", True, 10)
    add_line(f"Total USD equiv: {format_amount(cierre.total_efectivo_usd_equiv)}", "left", True, 10)

    add_line("-" * 32, "center")
    add_line("Realizado por: __________________", "left", False, 9)
    add_line("Recibido por: ___________________", "left", False, 9)
    add_line("Autorizado: _____________________", "left", False, 9)

    content_width = 70 * mm
    width = 80 * mm
    margin = (width - content_width) / 2
    top_margin = 6 * mm
    bottom_margin = 6 * mm
    logo_path = Path(__file__).resolve().parents[1] / "static" / "logopos.png"
    if not logo_path.exists():
        logo_path = Path(__file__).resolve().parents[1] / "static" / "logo_hollywood.png"
    logo_height = 45 * mm if logo_path.exists() else 0
    logo_spacing = 2 * mm if logo_height else 0

    def line_gap(size: int) -> float:
        return size + 5

    total_height = top_margin + bottom_margin + logo_height + logo_spacing
    total_height += sum(line_gap(size) for _, _, _, size in lines)
    total_height = total_height

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=(width, total_height))
    y = total_height - top_margin

    if logo_height:
        logo_width = 65 * mm
        pdf.drawImage(
            str(logo_path),
            (width - logo_width) / 2,
            y - logo_height,
            width=logo_width,
            height=logo_height,
            preserveAspectRatio=True,
            mask="auto",
        )
        y -= logo_height + logo_spacing

    for text, align, bold, size in lines:
        pdf.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        if "||" in text:
            left_text, right_text = text.split("||", 1)
            pdf.drawString(margin, y, left_text.strip())
            if right_text.strip():
                pdf.drawString(width / 2, y, right_text.strip())
        elif align == "center":
            pdf.drawCentredString(width / 2, y, text)
        elif align == "right":
            pdf.drawRightString(width - margin, y, text)
        else:
            pdf.drawString(margin, y, text)
        y -= line_gap(size)

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.read()


def _print_cierre_ticket(
    cierre: CierreCaja,
    tasa: Decimal,
    resumen: dict,
    total_bultos: Decimal,
    printer_name: str,
    copies: int,
    sumatra_override: Optional[str] = None,
) -> None:
    sumatra_path = _get_sumatra_path(sumatra_override)
    if not sumatra_path:
        return
    pdf_bytes = _build_cierre_ticket_pdf_bytes(cierre, tasa, resumen, total_bultos)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
        tmp_file.write(pdf_bytes)
        tmp_path = tmp_file.name
    try:
        for _ in range(max(copies, 1)):
            subprocess.run(
                [
                    str(sumatra_path),
                    "-print-to",
                    printer_name,
                    "-print-settings",
                    "noscale",
                    "-silent",
                    tmp_path,
                ],
                check=False,
            )
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except OSError:
            pass


def _generate_token(length: int = 6) -> str:
    import random
    import string

    return "".join(random.choice(string.digits) for _ in range(length))


def _send_reversion_email(
    subject: str,
    html_body: str,
    recipients: list[str],
    sender_email: Optional[str] = None,
    sender_name: Optional[str] = None,
    ) -> Optional[str]:
    if not recipients:
        return "No hay destinatarios activos"
    smtp_user = settings.SMTP_USER
    smtp_password = settings.SMTP_PASSWORD
    smtp_host = settings.SMTP_HOST
    smtp_port = settings.SMTP_PORT
    if not smtp_user or not smtp_password:
        env_path = Path(__file__).resolve().parents[2] / ".env"
        env_values = dotenv_values(env_path)
        def _env_get(key: str) -> str:
            return (env_values.get(key) or env_values.get(f"\ufeff{key}") or "").strip()
        smtp_user = smtp_user or _env_get("SMTP_USER")
        smtp_password = smtp_password or _env_get("SMTP_PASSWORD")
        smtp_host = smtp_host or _env_get("SMTP_HOST")
        smtp_port = smtp_port or int(_env_get("SMTP_PORT") or 0)
    if not smtp_user or not smtp_password:
        env_exists = env_path.exists()
        user_flag = "si" if smtp_user else "no"
        pass_flag = "si" if smtp_password else "no"
        return f"SMTP sin configurar (env={env_path}, existe={env_exists}, user={user_flag}, pass={pass_flag})"

    config = {
        "host": smtp_host or settings.SMTP_HOST,
        "port": smtp_port or settings.SMTP_PORT,
        "user": smtp_user,
        "password": smtp_password,
    }
    message = EmailMessage()
    message["Subject"] = subject
    from_email = sender_email or smtp_user
    if sender_name:
        message["From"] = f"{sender_name} <{from_email}>"
    else:
        message["From"] = from_email
    message["To"] = ", ".join(recipients)
    message.set_content("Se requiere un cliente de correo compatible con HTML.")
    message.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP(config["host"], config["port"]) as smtp:
            smtp.starttls()
            smtp.login(config["user"], config["password"])
            smtp.send_message(message)
    except Exception as exc:
        return f"Error SMTP: {exc.__class__.__name__}"
    return None

@router.get("/login")
def login_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/login")
def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    remember: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    identifier = username.strip().lower()
    user = db.query(User).filter(
        (func.lower(User.email) == identifier)
        | (func.lower(User.full_name) == identifier)
    ).first()
    if not user or not verify_password(password, user.hashed_password):
        return request.app.state.templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Credenciales incorrectas",
                "version": settings.UI_VERSION,
            },
            status_code=401,
        )
    if not user.is_active:
        return request.app.state.templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Usuario inactivo",
                "version": settings.UI_VERSION,
            },
            status_code=403,
        )

    # Keep sessions alive by default for POS usage (1 year).
    expires = timedelta(days=365)
    token = create_access_token({"sub": user.email}, expires_delta=expires)
    response = RedirectResponse("/home", status_code=302)
    max_age = int(expires.total_seconds())
    expires_at = datetime.now(timezone.utc) + expires
    host = request.url.hostname or ""
    cookie_domain = None
    if host and host not in {"localhost", "127.0.0.1"} and host.count(".") >= 1:
        cookie_domain = f".{host.split('.', 1)[1]}"
    response.set_cookie(
        "access_token",
        token,
        httponly=True,
        samesite="lax",
        max_age=max_age,
        expires=expires_at,
        path="/",
        domain=cookie_domain,
    )
    return response


@router.get("/logout")
def logout(request: Request):
    response = RedirectResponse("/login", status_code=302)
    host = request.url.hostname or ""
    cookie_domain = None
    if host and host not in {"localhost", "127.0.0.1"} and host.count(".") >= 1:
        cookie_domain = f".{host.split('.', 1)[1]}"
    response.delete_cookie("access_token", path="/", domain=cookie_domain)
    return response



@router.get("/")
def root():
    return RedirectResponse("/home", status_code=302)


@router.get("/home")
def home(request: Request, user: User = Depends(_require_admin_web)):
    return request.app.state.templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "user": user,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/finance")
def finance_home(
    request: Request,
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.finance")
    return request.app.state.templates.TemplateResponse(
        "finance.html",
        {
            "request": request,
            "user": user,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/inventory")
def inventory_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.inventory")
    edit_id = request.query_params.get("edit")
    show_inactive = request.query_params.get("show_inactive") in {"1", "true", "True"}
    edit_product = None
    if edit_id and edit_id.isdigit():
        edit_product = db.query(Producto).filter(Producto.id == int(edit_id)).first()
    productos_query = db.query(Producto).order_by(Producto.descripcion)
    if not show_inactive:
        productos_query = productos_query.filter(Producto.activo.is_(True))
    productos = productos_query.all()
    bodegas = db.query(Bodega).filter(Bodega.activo.is_(True)).order_by(Bodega.id).all()
    lineas = db.query(Linea).order_by(Linea.linea).all()
    segmentos = db.query(Segmento).order_by(Segmento.segmento).all()
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    product_ids = [p.id for p in productos]
    bodega_ids = [b.id for b in bodegas]
    balances = _balances_by_bodega(db, bodega_ids, product_ids)
    bodega_central = next((b for b in bodegas if (b.code or "").lower() == "central"), None)
    if not bodega_central:
        bodega_central = next((b for b in bodegas if "central" in (b.name or "").lower()), None)
    bodega_esteli = next((b for b in bodegas if (b.code or "").lower() == "esteli"), None)
    if not bodega_esteli:
        bodega_esteli = next((b for b in bodegas if "esteli" in (b.name or "").lower()), None)

    def _sum_for_bodega(bodega: Optional[Bodega]) -> tuple[Decimal, int]:
        if not bodega:
            return Decimal("0"), 0
        total_qty = Decimal("0")
        count_items = 0
        for producto in productos:
            qty = balances.get((producto.id, bodega.id), Decimal("0"))
            total_qty += qty
            if qty > 0:
                count_items += 1
        return total_qty, count_items

    central_qty, central_items = _sum_for_bodega(bodega_central)
    esteli_qty, esteli_items = _sum_for_bodega(bodega_esteli)
    global_qty = central_qty + esteli_qty
    global_items = len({p.id for p in productos if (balances.get((p.id, bodega_central.id), Decimal("0")) if bodega_central else Decimal("0")) > 0 or (balances.get((p.id, bodega_esteli.id), Decimal("0")) if bodega_esteli else Decimal("0")) > 0})
    return request.app.state.templates.TemplateResponse(
        "inventory.html",
        {
            "request": request,
            "user": user,
            "productos": productos,
            "bodegas": bodegas,
            "central_qty": float(central_qty),
            "esteli_qty": float(esteli_qty),
            "global_qty": float(global_qty),
            "central_items": central_items,
            "esteli_items": esteli_items,
            "global_items": global_items,
            "lineas": lineas,
            "segmentos": segmentos,
            "edit_product": edit_product,
            "rate_today": rate_today,
            "error": error,
            "success": success,
            "show_inactive": show_inactive,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/inventory/caliente")
def inventory_caliente(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.inventory.caliente")
    q = (request.query_params.get("q") or "").strip()
    scope_param = (request.query_params.get("scope") or "central").strip().lower()
    scope = scope_param
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    rate_value = Decimal(str(rate_today.rate)) if rate_today and rate_today.rate else Decimal("0")

    productos_query = (
        db.query(Producto)
        .outerjoin(SaldoProducto)
        .filter(Producto.activo.is_(True))
    )
    if q:
        q_like = f"%{q.lower()}%"
        productos_query = productos_query.filter(
            or_(
                func.lower(Producto.cod_producto).like(q_like),
                func.lower(Producto.descripcion).like(q_like),
            )
        )
    productos = productos_query.order_by(Producto.descripcion).all()

    branches = db.query(Branch).filter(Branch.code.in_(["central", "esteli"])).all()
    branch_map = {b.code.lower(): b for b in branches if b.code}
    central_branch = branch_map.get("central")
    esteli_branch = branch_map.get("esteli")
    bodegas_query = db.query(Bodega).filter(Bodega.activo.is_(True))
    if branches:
        bodegas_query = bodegas_query.filter(Bodega.branch_id.in_([b.id for b in branches]))
    bodegas = bodegas_query.all()
    bodega_map = {b.branch_id: b for b in bodegas}

    branch, user_bodega = _resolve_branch_bodega(db, user)
    if scope not in {"central", "esteli", "ambas"}:
        scope = "central"

    def _balances_by_bodega(bodega_ids: list[int], product_ids: list[int]) -> dict[tuple[int, int], Decimal]:
        if not bodega_ids or not product_ids:
            return {}
        ingreso_rows = (
            db.query(IngresoItem.producto_id, IngresoInventario.bodega_id, func.sum(IngresoItem.cantidad))
            .join(IngresoInventario, IngresoInventario.id == IngresoItem.ingreso_id)
            .filter(IngresoInventario.bodega_id.in_(bodega_ids))
            .filter(IngresoItem.producto_id.in_(product_ids))
            .group_by(IngresoItem.producto_id, IngresoInventario.bodega_id)
            .all()
        )
        egreso_rows = (
            db.query(EgresoItem.producto_id, EgresoInventario.bodega_id, func.sum(EgresoItem.cantidad))
            .join(EgresoInventario, EgresoInventario.id == EgresoItem.egreso_id)
            .filter(EgresoInventario.bodega_id.in_(bodega_ids))
            .filter(EgresoItem.producto_id.in_(product_ids))
            .group_by(EgresoItem.producto_id, EgresoInventario.bodega_id)
            .all()
        )
        venta_rows = (
            db.query(VentaItem.producto_id, VentaFactura.bodega_id, func.sum(VentaItem.cantidad))
            .join(VentaFactura, VentaFactura.id == VentaItem.factura_id)
            .filter(VentaFactura.bodega_id.in_(bodega_ids))
            .filter(VentaItem.producto_id.in_(product_ids))
            .filter(VentaFactura.estado != "ANULADA")
            .group_by(VentaItem.producto_id, VentaFactura.bodega_id)
            .all()
        )
        balances: dict[tuple[int, int], Decimal] = {}
        for producto_id, bodega_id, qty in ingreso_rows:
            balances[(producto_id, bodega_id)] = Decimal(str(qty or 0))
        for producto_id, bodega_id, qty in egreso_rows:
            balances[(producto_id, bodega_id)] = balances.get((producto_id, bodega_id), Decimal("0")) - Decimal(str(qty or 0))
        for producto_id, bodega_id, qty in venta_rows:
            balances[(producto_id, bodega_id)] = balances.get((producto_id, bodega_id), Decimal("0")) - Decimal(str(qty or 0))
        return balances

    product_ids = [p.id for p in productos]
    bodega_ids = [b.id for b in bodegas]
    balances = _balances_by_bodega(bodega_ids, product_ids)

    productos_view = []
    for producto in productos:
        price_usd = Decimal(str(producto.precio_venta1_usd or 0))
        price_cs = Decimal(str(producto.precio_venta1 or 0))
        if price_usd == 0 and price_cs and rate_value:
            price_usd = (price_cs / rate_value).quantize(Decimal("0.01"))
        if price_cs == 0 and price_usd and rate_value:
            price_cs = (price_usd * rate_value).quantize(Decimal("0.01"))
        item = {
            "id": producto.id,
            "codigo": producto.cod_producto,
            "descripcion": producto.descripcion,
            "precio_usd": float(price_usd or 0),
            "precio_cs": float(price_cs or 0),
        }
        if scope == "ambas":
            rows = []
            if central_branch:
                central_bodega = bodega_map.get(central_branch.id)
                qty = balances.get((producto.id, central_bodega.id), Decimal("0")) if central_bodega else Decimal("0")
                rows.append({"label": central_branch.name, "existencia": float(qty or 0)})
            if esteli_branch:
                esteli_bodega = bodega_map.get(esteli_branch.id)
                qty = balances.get((producto.id, esteli_bodega.id), Decimal("0")) if esteli_bodega else Decimal("0")
                rows.append({"label": esteli_branch.name, "existencia": float(qty or 0)})
            total_qty = sum(Decimal(str(row["existencia"])) for row in rows)
            item["existencias"] = rows
            item["existencia_total"] = float(total_qty or 0)
        else:
            selected_branch = central_branch if scope == "central" else esteli_branch if scope == "esteli" else branch
            selected_bodega = None
            if selected_branch:
                selected_bodega = bodega_map.get(selected_branch.id)
            if scope not in {"central", "esteli"} and user_bodega:
                selected_bodega = user_bodega
            qty = balances.get((producto.id, selected_bodega.id), Decimal("0")) if selected_bodega else Decimal("0")
            item["existencia"] = float(qty or 0)
            item["scope_label"] = selected_branch.name if selected_branch else "Bodega"
        productos_view.append(item)

    template_name = "inventory_caliente_grid.html" if request.headers.get("HX-Request") else "inventory_caliente.html"
    return request.app.state.templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "user": user,
            "q": q,
            "productos": productos_view,
            "rate_today": rate_today,
            "scope": scope,
            "scope_param": scope_param,
            "current_branch": branch,
            "central_branch": central_branch,
            "esteli_branch": esteli_branch,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/inventory/ingresos")
def inventory_ingresos_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.inventory.ingresos")
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    start_date = _parse_date(request.query_params.get("start_date"))
    end_date = _parse_date(request.query_params.get("end_date"))
    if not start_date and not end_date:
        end_date = local_today()
        start_date = end_date - timedelta(days=30)
    ingresos_query = db.query(IngresoInventario)
    if start_date:
        ingresos_query = ingresos_query.filter(IngresoInventario.fecha >= start_date)
    if end_date:
        ingresos_query = ingresos_query.filter(IngresoInventario.fecha <= end_date)
    ingresos = (
        ingresos_query.order_by(IngresoInventario.fecha.desc(), IngresoInventario.id.desc())
        .all()
    )
    tipos = db.query(IngresoTipo).order_by(IngresoTipo.nombre).all()
    bodegas = db.query(Bodega).order_by(Bodega.name).all()
    proveedores = db.query(Proveedor).order_by(Proveedor.nombre).all()
    productos = (
        db.query(Producto)
        .filter(Producto.activo.is_(True))
        .order_by(Producto.descripcion)
        .all()
    )
    product_ids = [p.id for p in productos]
    bodega_ids = [b.id for b in bodegas]
    balances = _balances_by_bodega(db, bodega_ids, product_ids)
    saldos_por_bodega: dict[int, dict[int, float]] = {}
    for producto in productos:
        per_bodega: dict[int, float] = {}
        for bodega in bodegas:
            qty = balances.get((producto.id, bodega.id), Decimal("0"))
            per_bodega[bodega.id] = float(qty or 0)
        saldos_por_bodega[producto.id] = per_bodega
    lineas = db.query(Linea).order_by(Linea.linea).all()
    segmentos = db.query(Segmento).order_by(Segmento.segmento).all()
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    print_id = request.query_params.get("print_id")
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    return request.app.state.templates.TemplateResponse(
        "inventory_ingresos.html",
        {
            "request": request,
            "user": user,
            "ingresos": ingresos,
            "tipos": tipos,
            "bodegas": bodegas,
            "proveedores": proveedores,
            "productos": productos,
            "saldos_por_bodega": saldos_por_bodega,
            "lineas": lineas,
            "segmentos": segmentos,
            "rate_today": rate_today,
            "error": error,
            "start_date": start_date.isoformat() if start_date else "",
            "end_date": end_date.isoformat() if end_date else "",
            "success": success,
            "print_id": print_id,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/inventory/egresos")
def inventory_egresos_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.inventory.egresos")
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    start_date = _parse_date(request.query_params.get("start_date"))
    end_date = _parse_date(request.query_params.get("end_date"))
    if not start_date and not end_date:
        end_date = local_today()
        start_date = end_date - timedelta(days=30)
    egresos_query = db.query(EgresoInventario)
    if start_date:
        egresos_query = egresos_query.filter(EgresoInventario.fecha >= start_date)
    if end_date:
        egresos_query = egresos_query.filter(EgresoInventario.fecha <= end_date)
    egresos = (
        egresos_query.order_by(EgresoInventario.fecha.desc(), EgresoInventario.id.desc())
        .all()
    )
    tipos = db.query(EgresoTipo).order_by(EgresoTipo.nombre).all()
    bodegas = db.query(Bodega).order_by(Bodega.name).all()
    productos = (
        db.query(Producto)
        .filter(Producto.activo.is_(True))
        .order_by(Producto.descripcion)
        .all()
    )
    product_ids = [p.id for p in productos]
    bodega_ids = [b.id for b in bodegas]
    balances = _balances_by_bodega(db, bodega_ids, product_ids)
    saldos_por_bodega: dict[int, dict[int, float]] = {}
    for producto in productos:
        per_bodega: dict[int, float] = {}
        for bodega in bodegas:
            qty = balances.get((producto.id, bodega.id), Decimal("0"))
            per_bodega[bodega.id] = float(qty or 0)
        saldos_por_bodega[producto.id] = per_bodega
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    print_id = request.query_params.get("print_id")
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    return request.app.state.templates.TemplateResponse(
        "inventory_egresos.html",
        {
            "request": request,
            "user": user,
            "egresos": egresos,
            "tipos": tipos,
            "bodegas": bodegas,
            "productos": productos,
            "saldos_por_bodega": saldos_por_bodega,
            "rate_today": rate_today,
            "error": error,
            "start_date": start_date.isoformat() if start_date else "",
            "end_date": end_date.isoformat() if end_date else "",
            "success": success,
            "print_id": print_id,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/sales")
def sales_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.sales")
    productos = (
        db.query(Producto)
        .filter(Producto.activo.is_(True))
        .order_by(Producto.descripcion)
        .all()
    )
    clientes = db.query(Cliente).order_by(Cliente.nombre).all()
    formas_pago = db.query(FormaPago).order_by(FormaPago.nombre).all()
    bancos = db.query(Banco).order_by(Banco.nombre).all()
    cuentas = db.query(CuentaBancaria).order_by(CuentaBancaria.banco_id).all()
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    print_id = request.query_params.get("print_id")
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    branch, bodega = _resolve_branch_bodega(db, user)
    vendedores = _vendedores_for_bodega(db, bodega)
    next_invoice = None
    if branch and bodega:
        last_factura = (
            db.query(VentaFactura)
            .filter(VentaFactura.bodega_id == bodega.id)
            .order_by(VentaFactura.secuencia.desc())
            .first()
        )
        next_seq = (last_factura.secuencia if last_factura else 0) + 1
        branch_code = (branch.code or "").lower()
        prefix = "C" if branch_code == "central" else "E" if branch_code == "esteli" else branch_code[:1].upper()
        width = 6
        next_invoice = f"{prefix}-{next_seq:0{width}d}"
    pos_print = (
        db.query(PosPrintSetting)
        .filter(PosPrintSetting.branch_id == branch.id)
        .first()
        if branch
        else None
    )
    return request.app.state.templates.TemplateResponse(
        "sales.html",
        {
            "request": request,
            "user": user,
            "productos": productos,
            "clientes": clientes,
            "vendedores": vendedores,
            "formas_pago": formas_pago,
            "bancos": bancos,
            "cuentas": cuentas,
            "rate_today": rate_today,
            "error": error,
            "success": success,
            "print_id": print_id,
            "next_invoice": next_invoice,
            "pos_print": pos_print,
            "default_vendedor_id": _default_vendedor_id(db, bodega),
            "version": settings.UI_VERSION,
        },
    )


@router.get("/sales/utilitario")
def sales_utilitario(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.sales.utilitario")
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    start_date = _parse_date(request.query_params.get("start_date"))
    end_date = _parse_date(request.query_params.get("end_date"))
    branch_id = request.query_params.get("branch_id") or "all"
    cliente_q = (request.query_params.get("cliente") or "").strip()
    vendedor_q = (request.query_params.get("vendedor") or "").strip()
    producto_q = (request.query_params.get("producto") or "").strip()
    if not start_date and not end_date:
        start_date = local_today()
        end_date = local_today()

    ventas_query = (
        db.query(VentaFactura)
        .join(Bodega, Bodega.id == VentaFactura.bodega_id, isouter=True)
        .join(Branch, Branch.id == Bodega.branch_id, isouter=True)
    )
    if branch_id and branch_id != "all":
        try:
            ventas_query = ventas_query.filter(Branch.id == int(branch_id))
        except ValueError:
            pass
    if start_date:
        start_dt = datetime.combine(start_date, datetime.min.time())
        ventas_query = ventas_query.filter(VentaFactura.fecha >= start_dt)
    if end_date:
        end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
        ventas_query = ventas_query.filter(VentaFactura.fecha < end_dt)
    if cliente_q:
        ventas_query = ventas_query.join(Cliente, isouter=True).filter(
            func.lower(Cliente.nombre).like(f"%{cliente_q.lower()}%")
        )
    if vendedor_q:
        ventas_query = ventas_query.join(Vendedor, isouter=True).filter(
            func.lower(Vendedor.nombre).like(f"%{vendedor_q.lower()}%")
        )
    if producto_q:
        ventas_query = (
            ventas_query.join(VentaItem)
            .join(Producto)
            .filter(
                (func.lower(Producto.descripcion).like(f"%{producto_q.lower()}%"))
                | (func.lower(Producto.cod_producto).like(f"%{producto_q.lower()}%"))
            )
            .distinct()
        )
    ventas = (
        ventas_query.order_by(VentaFactura.fecha.desc(), VentaFactura.id.desc()).all()
    )
    _, bodega = _resolve_branch_bodega(db, user)
    vendedores = _vendedores_for_bodega(db, bodega)
    branches = db.query(Branch).order_by(Branch.name).all()

    return request.app.state.templates.TemplateResponse(
        "sales_utilitario.html",
        {
            "request": request,
            "user": user,
            "ventas": ventas,
            "vendedores": vendedores,
            "branches": branches,
            "start_date": start_date.isoformat() if start_date else "",
            "end_date": end_date.isoformat() if end_date else "",
            "branch_id": branch_id,
            "cliente_q": cliente_q,
            "vendedor_q": vendedor_q,
            "producto_q": producto_q,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/sales/roc")
def sales_roc(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.sales.roc")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    print_id = request.query_params.get("print_id")
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    today_value = local_today()
    start_date = today_value
    end_date = today_value
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today_value
            end_date = today_value
    branch, bodega = _resolve_branch_bodega(db, user)
    rubros = db.query(ReciboRubro).filter(ReciboRubro.activo.is_(True)).order_by(ReciboRubro.nombre).all()
    motivos = db.query(ReciboMotivo).filter(ReciboMotivo.activo.is_(True)).order_by(ReciboMotivo.tipo, ReciboMotivo.nombre).all()
    recibos_query = db.query(ReciboCaja)
    if bodega:
        recibos_query = recibos_query.filter(ReciboCaja.bodega_id == bodega.id)
    recibos_query = recibos_query.filter(ReciboCaja.fecha.between(start_date, end_date))
    recibos = recibos_query.order_by(ReciboCaja.fecha.desc(), ReciboCaja.id.desc()).limit(200).all()
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    return request.app.state.templates.TemplateResponse(
        "sales_roc.html",
        {
            "request": request,
            "user": user,
            "rubros": rubros,
            "motivos": motivos,
            "recibos": recibos,
            "rate_today": rate_today,
            "branch": branch,
            "bodega": bodega,
            "today": today_value.isoformat(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "error": error,
            "success": success,
            "print_id": print_id,
            "version": settings.UI_VERSION,
        },
    )

@router.get("/sales/cierre")
def sales_cierre(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.sales.cierre")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    print_id = request.query_params.get("print_id")
    fecha_raw = request.query_params.get("fecha")
    fecha_value = local_today()
    if fecha_raw:
        try:
            fecha_value = date.fromisoformat(str(fecha_raw))
        except ValueError:
            fecha_value = local_today()

    branch, bodega = _resolve_branch_bodega(db, user)
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= fecha_value)
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

    def to_usd(moneda: str, monto_cs: Decimal, monto_usd: Decimal) -> Decimal:
        if moneda == "USD":
            return Decimal(str(monto_usd or 0))
        return (Decimal(str(monto_cs or 0)) / tasa) if tasa else Decimal("0")

    ventas_query = db.query(VentaFactura).filter(func.date(VentaFactura.fecha) == fecha_value)
    if bodega:
        ventas_query = ventas_query.filter(VentaFactura.bodega_id == bodega.id)
    ventas_query = ventas_query.filter(VentaFactura.estado != "ANULADA")
    ventas = ventas_query.all()
    total_ventas_usd = sum(
        to_usd(f.moneda or "CS", f.total_cs or 0, f.total_usd or 0) for f in ventas
    )

    recibos_query = db.query(ReciboCaja).filter(func.date(ReciboCaja.fecha) == fecha_value)
    if bodega:
        recibos_query = recibos_query.filter(ReciboCaja.bodega_id == bodega.id)
    recibos_query = recibos_query.filter(ReciboCaja.afecta_caja.is_(True))
    recibos = recibos_query.all()
    total_ingresos_usd = sum(
        to_usd(r.moneda or "CS", r.monto_cs or 0, r.monto_usd or 0)
        for r in recibos
        if r.tipo == "INGRESO"
    )
    total_egresos_usd = sum(
        to_usd(r.moneda or "CS", r.monto_cs or 0, r.monto_usd or 0)
        for r in recibos
        if r.tipo == "EGRESO"
    )

    depositos_query = db.query(DepositoCliente).filter(func.date(DepositoCliente.fecha) == fecha_value)
    if bodega:
        depositos_query = depositos_query.filter(DepositoCliente.bodega_id == bodega.id)
    depositos = depositos_query.all()
    total_depositos_usd = sum(
        to_usd(d.moneda or "CS", d.monto_cs or 0, d.monto_usd or 0) for d in depositos
    )

    creditos_query = db.query(VentaFactura).filter(
        func.date(VentaFactura.fecha) == fecha_value,
        VentaFactura.estado != "ANULADA",
        VentaFactura.estado_cobranza == "PENDIENTE",
    )
    if bodega:
        creditos_query = creditos_query.filter(VentaFactura.bodega_id == bodega.id)
    creditos = creditos_query.all()
    total_creditos_usd = Decimal("0")
    for factura in creditos:
        if (factura.moneda or "CS") == "USD":
            paid_usd = sum(Decimal(str(a.monto_usd or 0)) for a in factura.abonos)
            due_usd = Decimal(str(factura.total_usd or 0))
            saldo_usd = max(due_usd - paid_usd, Decimal("0"))
            if saldo_usd > 0:
                total_creditos_usd += saldo_usd
        else:
            paid_cs = sum(Decimal(str(a.monto_cs or 0)) for a in factura.abonos)
            due_cs = Decimal(str(factura.total_cs or 0))
            saldo_cs = max(due_cs - paid_cs, Decimal("0"))
            if saldo_cs > 0:
                total_creditos_usd += (saldo_cs / tasa) if tasa else Decimal("0")

    total_calculado_usd = (
        Decimal(str(total_ventas_usd))
        - Decimal(str(total_egresos_usd))
        + Decimal(str(total_ingresos_usd))
        - Decimal(str(total_depositos_usd))
        - Decimal(str(total_creditos_usd))
    )

    denominaciones_cs = [0.5, 1, 5, 10, 20, 50, 100, 200, 500, 1000]
    denominaciones_usd = [1, 2, 5, 10, 20, 50, 100]

    return request.app.state.templates.TemplateResponse(
        "sales_cierre.html",
        {
            "request": request,
            "user": user,
            "branch": branch,
            "bodega": bodega,
            "fecha": fecha_value.isoformat(),
            "rate_today": rate_today,
            "tasa": float(tasa) if tasa else 0,
            "total_ventas_usd": float(total_ventas_usd),
            "total_ingresos_usd": float(total_ingresos_usd),
            "total_egresos_usd": float(total_egresos_usd),
            "total_depositos_usd": float(total_depositos_usd),
            "total_creditos_usd": float(total_creditos_usd),
            "total_calculado_usd": float(total_calculado_usd),
            "denominaciones_cs": denominaciones_cs,
            "denominaciones_usd": denominaciones_usd,
            "error": error,
            "success": success,
            "print_id": print_id,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/sales/cierre")
async def sales_cierre_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    fecha_raw = form.get("fecha")
    fecha_value = local_today()
    if fecha_raw:
        try:
            fecha_value = date.fromisoformat(str(fecha_raw))
        except ValueError:
            fecha_value = local_today()

    branch, bodega = _resolve_branch_bodega(db, user)
    if not branch or not bodega:
        return RedirectResponse("/sales/cierre?error=Usuario+sin+sucursal+o+bodega", status_code=303)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= fecha_value)
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

    detalle_cs = {}
    detalle_usd = {}
    total_cs = Decimal("0")
    total_usd = Decimal("0")
    def parse_qty(val: str) -> Decimal:
        raw = re.sub(r"[^0-9.,-]", "", str(val or "0"))
        if "," in raw and "." in raw:
            if raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "," in raw and "." not in raw:
            raw = raw.replace(",", "")
        try:
            return Decimal(raw or "0")
        except Exception:
            return Decimal("0")

    for key, value in form.items():
        if key.startswith("cs_"):
            denom = Decimal(key.replace("cs_", "").replace("_", "."))
            qty = parse_qty(str(value))
            detalle_cs[str(denom)] = float(qty)
            total_cs += denom * qty
        if key.startswith("usd_"):
            denom = Decimal(key.replace("usd_", "").replace("_", "."))
            qty = parse_qty(str(value))
            detalle_usd[str(denom)] = float(qty)
            total_usd += denom * qty

    total_usd_equiv = total_usd + (total_cs / tasa if tasa else Decimal("0"))

    def to_usd(moneda: str, monto_cs: Decimal, monto_usd: Decimal) -> Decimal:
        if moneda == "USD":
            return Decimal(str(monto_usd or 0))
        return (Decimal(str(monto_cs or 0)) / tasa) if tasa else Decimal("0")

    ventas_query = db.query(VentaFactura).filter(func.date(VentaFactura.fecha) == fecha_value)
    ventas_query = ventas_query.filter(VentaFactura.estado != "ANULADA")
    if bodega:
        ventas_query = ventas_query.filter(VentaFactura.bodega_id == bodega.id)
    ventas = ventas_query.all()
    total_ventas_usd = sum(
        to_usd(f.moneda or "CS", f.total_cs or 0, f.total_usd or 0) for f in ventas
    )

    recibos_query = db.query(ReciboCaja).filter(func.date(ReciboCaja.fecha) == fecha_value)
    if bodega:
        recibos_query = recibos_query.filter(ReciboCaja.bodega_id == bodega.id)
    recibos_query = recibos_query.filter(ReciboCaja.afecta_caja.is_(True))
    recibos = recibos_query.all()
    total_ingresos_usd = sum(
        to_usd(r.moneda or "CS", r.monto_cs or 0, r.monto_usd or 0)
        for r in recibos
        if r.tipo == "INGRESO"
    )
    total_egresos_usd = sum(
        to_usd(r.moneda or "CS", r.monto_cs or 0, r.monto_usd or 0)
        for r in recibos
        if r.tipo == "EGRESO"
    )

    depositos_query = db.query(DepositoCliente).filter(func.date(DepositoCliente.fecha) == fecha_value)
    if bodega:
        depositos_query = depositos_query.filter(DepositoCliente.bodega_id == bodega.id)
    depositos = depositos_query.all()
    total_depositos_usd = sum(
        to_usd(d.moneda or "CS", d.monto_cs or 0, d.monto_usd or 0) for d in depositos
    )

    creditos_query = db.query(VentaFactura).filter(
        func.date(VentaFactura.fecha) == fecha_value,
        VentaFactura.estado != "ANULADA",
        VentaFactura.estado_cobranza == "PENDIENTE",
    )
    if bodega:
        creditos_query = creditos_query.filter(VentaFactura.bodega_id == bodega.id)
    creditos = creditos_query.all()
    total_creditos_usd = Decimal("0")
    for factura in creditos:
        if (factura.moneda or "CS") == "USD":
            paid_usd = sum(Decimal(str(a.monto_usd or 0)) for a in factura.abonos)
            due_usd = Decimal(str(factura.total_usd or 0))
            saldo_usd = max(due_usd - paid_usd, Decimal("0"))
            if saldo_usd > 0:
                total_creditos_usd += saldo_usd
        else:
            paid_cs = sum(Decimal(str(a.monto_cs or 0)) for a in factura.abonos)
            due_cs = Decimal(str(factura.total_cs or 0))
            saldo_cs = max(due_cs - paid_cs, Decimal("0"))
            if saldo_cs > 0:
                total_creditos_usd += (saldo_cs / tasa) if tasa else Decimal("0")

    total_calculado_usd = (
        Decimal(str(total_ventas_usd))
        - Decimal(str(total_egresos_usd))
        + Decimal(str(total_ingresos_usd))
        - Decimal(str(total_depositos_usd))
        - Decimal(str(total_creditos_usd))
    )
    diferencia = total_usd_equiv - total_calculado_usd

    cierre = CierreCaja(
        branch_id=branch.id,
        bodega_id=bodega.id,
        fecha=fecha_value,
        detalle_cs=json.dumps(detalle_cs),
        detalle_usd=json.dumps(detalle_usd),
        total_efectivo_cs=total_cs,
        total_efectivo_usd=total_usd,
        total_efectivo_usd_equiv=total_usd_equiv,
        total_ventas_usd=total_ventas_usd,
        total_ingresos_usd=total_ingresos_usd,
        total_egresos_usd=total_egresos_usd,
        total_depositos_usd=total_depositos_usd,
        total_creditos_usd=total_creditos_usd,
        total_calculado_usd=total_calculado_usd,
        diferencia_usd=diferencia,
        usuario_registro=user.full_name,
    )
    db.add(cierre)
    db.commit()
    pos_print = (
        db.query(PosPrintSetting)
        .filter(PosPrintSetting.branch_id == branch.id)
        .first()
    )
    if pos_print and pos_print.cierre_auto_print:
        try:
            resumen = {
                "ventas_usd": total_ventas_usd,
                "ingresos_usd": total_ingresos_usd,
                "egresos_usd": total_egresos_usd,
                "depositos_usd": total_depositos_usd,
                "creditos_usd": total_creditos_usd,
                "total_calculado_usd": total_calculado_usd,
            }
            total_bultos = (
                db.query(func.coalesce(func.sum(VentaItem.cantidad), 0))
                .join(VentaFactura)
                .filter(
                    func.date(VentaFactura.fecha) == fecha_value,
                    VentaFactura.estado != "ANULADA",
                    VentaFactura.bodega_id == bodega.id,
                )
                .scalar()
            )
            _print_cierre_ticket(
                cierre,
                tasa,
                resumen,
                Decimal(str(total_bultos or 0)),
                pos_print.cierre_printer_name or pos_print.printer_name,
                pos_print.cierre_copies or 1,
                pos_print.sumatra_path,
            )
        except Exception:
            pass
    return RedirectResponse(f"/sales/cierre?success=Cierre+registrado&print_id={cierre.id}", status_code=303)


@router.get("/sales/cierre/{cierre_id}/pdf")
def sales_cierre_pdf(
    cierre_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    cierre = db.query(CierreCaja).filter(CierreCaja.id == cierre_id).first()
    if not cierre:
        return JSONResponse({"ok": False, "message": "Cierre no encontrado"}, status_code=404)
    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and cierre.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Cierre fuera de tu bodega"}, status_code=403)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= cierre.fecha)
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")
    resumen = {
        "ventas_usd": cierre.total_ventas_usd,
        "ingresos_usd": cierre.total_ingresos_usd,
        "egresos_usd": cierre.total_egresos_usd,
        "depositos_usd": cierre.total_depositos_usd,
        "creditos_usd": cierre.total_creditos_usd,
        "total_calculado_usd": cierre.total_calculado_usd,
    }
    total_bultos = (
        db.query(func.coalesce(func.sum(VentaItem.cantidad), 0))
        .join(VentaFactura)
        .filter(
            func.date(VentaFactura.fecha) == cierre.fecha,
            VentaFactura.estado != "ANULADA",
            VentaFactura.bodega_id == cierre.bodega_id,
        )
        .scalar()
    )
    pdf_bytes = _build_cierre_ticket_pdf_bytes(
        cierre,
        tasa,
        resumen,
        Decimal(str(total_bultos or 0)),
    )
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename=cierre_{cierre.fecha}.pdf"},
    )

@router.post("/sales/roc")
async def sales_roc_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    tipo = (form.get("tipo") or "EGRESO").upper()
    rubro_id = form.get("rubro_id")
    motivo_id = form.get("motivo_id")
    descripcion = (form.get("descripcion") or "").strip()
    fecha_raw = form.get("fecha")
    moneda = (form.get("moneda") or "CS").upper()
    monto_raw = form.get("monto")
    afecta_caja = form.get("afecta_caja") == "on"

    if tipo not in {"INGRESO", "EGRESO"}:
        return RedirectResponse("/sales/roc?error=Tipo+no+valido", status_code=303)
    if not rubro_id or not motivo_id or not monto_raw:
        return RedirectResponse("/sales/roc?error=Datos+incompletos", status_code=303)
    if moneda not in {"CS", "USD"}:
        return RedirectResponse("/sales/roc?error=Moneda+no+valida", status_code=303)

    try:
        monto = float(monto_raw)
    except ValueError:
        monto = 0.0
    if monto <= 0:
        return RedirectResponse("/sales/roc?error=Monto+no+valido", status_code=303)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if moneda == "USD" and not rate_today:
        return RedirectResponse("/sales/roc?error=Tasa+de+cambio+no+configurada", status_code=303)
    tasa = float(rate_today.rate) if rate_today else 0

    branch, bodega = _resolve_branch_bodega(db, user)
    if not branch:
        return RedirectResponse("/sales/roc?error=Usuario+sin+sucursal+asignada", status_code=303)
    if not bodega:
        return RedirectResponse("/sales/roc?error=Bodega+no+configurada+para+la+sucursal", status_code=303)

    rubro = db.query(ReciboRubro).filter(ReciboRubro.id == int(rubro_id), ReciboRubro.activo.is_(True)).first()
    motivo = db.query(ReciboMotivo).filter(ReciboMotivo.id == int(motivo_id), ReciboMotivo.tipo == tipo, ReciboMotivo.activo.is_(True)).first()
    if not rubro or not motivo:
        return RedirectResponse("/sales/roc?error=Rubro+o+motivo+no+valido", status_code=303)

    if fecha_raw:
        try:
            fecha_value = date.fromisoformat(str(fecha_raw).split("T")[0])
        except ValueError:
            fecha_value = local_today()
    else:
        fecha_value = local_today()

    last_recibo = (
        db.query(ReciboCaja)
        .filter(ReciboCaja.bodega_id == bodega.id)
        .order_by(ReciboCaja.secuencia.desc())
        .first()
    )
    next_seq = (last_recibo.secuencia if last_recibo else 0) + 1
    branch_code = (branch.code or "").lower()
    prefix = "ROC-C" if branch_code == "central" else "ROC-E" if branch_code == "esteli" else f"ROC-{branch_code[:1].upper()}"
    numero = f"{prefix}-{next_seq:05d}"

    monto_usd = Decimal("0")
    monto_cs = Decimal("0")
    if moneda == "USD":
        monto_usd = Decimal(str(monto))
        monto_cs = Decimal(str(monto * tasa))
    else:
        monto_cs = Decimal(str(monto))
        monto_usd = Decimal(str(monto / tasa)) if tasa else Decimal("0")

    recibo = ReciboCaja(
        secuencia=next_seq,
        numero=numero,
        branch_id=branch.id,
        bodega_id=bodega.id,
        tipo=tipo,
        rubro_id=rubro.id,
        motivo_id=motivo.id,
        descripcion=descripcion,
        fecha=fecha_value,
        moneda=moneda,
        tasa_cambio=tasa if moneda == "USD" else None,
        monto_usd=monto_usd,
        monto_cs=monto_cs,
        afecta_caja=afecta_caja,
        usuario_registro=user.full_name,
    )
    db.add(recibo)

    if afecta_caja:
        caja = (
            db.query(CajaDiaria)
            .filter(
                CajaDiaria.branch_id == branch.id,
                CajaDiaria.bodega_id == bodega.id,
                CajaDiaria.fecha == fecha_value,
            )
            .first()
        )
        if not caja:
            caja = CajaDiaria(
                branch_id=branch.id,
                bodega_id=bodega.id,
                fecha=fecha_value,
                saldo_usd=Decimal("0"),
                saldo_cs=Decimal("0"),
            )
            db.add(caja)
        if tipo == "INGRESO":
            caja.saldo_usd = Decimal(str(caja.saldo_usd or 0)) + monto_usd
            caja.saldo_cs = Decimal(str(caja.saldo_cs or 0)) + monto_cs
        else:
            caja.saldo_usd = Decimal(str(caja.saldo_usd or 0)) - monto_usd
            caja.saldo_cs = Decimal(str(caja.saldo_cs or 0)) - monto_cs

    db.commit()
    print_id = recibo.id
    pos_print = (
        db.query(PosPrintSetting)
        .filter(PosPrintSetting.branch_id == branch.id)
        .first()
    )
    if pos_print and pos_print.roc_auto_print:
        try:
            _print_roc_ticket(
                recibo,
                pos_print.roc_printer_name or pos_print.printer_name,
                pos_print.roc_copies or pos_print.copies,
                pos_print.sumatra_path,
            )
        except Exception:
            pass
    return RedirectResponse(f"/sales/roc?success=Recibo+registrado&print_id={print_id}", status_code=303)


@router.get("/sales/roc/{recibo_id}/pdf")
def sales_roc_pdf(
    recibo_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    recibo = db.query(ReciboCaja).filter(ReciboCaja.id == recibo_id).first()
    if not recibo:
        return JSONResponse({"ok": False, "message": "Recibo no encontrado"}, status_code=404)
    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and recibo.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Recibo fuera de tu bodega"}, status_code=403)
    pdf_bytes = _build_roc_ticket_pdf_bytes(recibo)
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename=roc_{recibo.numero}.pdf"},
    )


@router.get("/sales/depositos")
def sales_depositos(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.sales.depositos")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    vendedor_q = request.query_params.get("vendedor_id")
    banco_q = request.query_params.get("banco_id")
    moneda_q = request.query_params.get("moneda")
    today_value = local_today()
    start_date = today_value
    end_date = today_value
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today_value
            end_date = today_value

    branch, bodega = _resolve_branch_bodega(db, user)
    vendedores = _vendedores_for_bodega(db, bodega)
    bancos = db.query(Banco).order_by(Banco.nombre).all()
    cuentas = db.query(CuentaBancaria).order_by(CuentaBancaria.banco_id).all()

    depositos_query = db.query(DepositoCliente)
    if bodega:
        depositos_query = depositos_query.filter(DepositoCliente.bodega_id == bodega.id)
    depositos_query = depositos_query.filter(DepositoCliente.fecha.between(start_date, end_date))
    if vendedor_q:
        depositos_query = depositos_query.filter(DepositoCliente.vendedor_id == int(vendedor_q))
    if banco_q:
        depositos_query = depositos_query.filter(DepositoCliente.banco_id == int(banco_q))
    if moneda_q:
        depositos_query = depositos_query.filter(DepositoCliente.moneda == moneda_q.upper())
    depositos = depositos_query.order_by(DepositoCliente.fecha.desc(), DepositoCliente.id.desc()).all()

    summary = {}
    total_cs = Decimal("0")
    total_usd = Decimal("0")
    for dep in depositos:
        key = (dep.banco_id, dep.moneda)
        if key not in summary:
            summary[key] = {
                "banco": dep.banco.nombre if dep.banco else "-",
                "moneda": dep.moneda,
                "count": 0,
                "total": Decimal("0"),
            }
        summary[key]["count"] += 1
        monto_cs = Decimal(str(dep.monto_cs or 0))
        monto_usd = Decimal(str(dep.monto_usd or 0))
        if dep.moneda == "USD":
            summary[key]["total"] += monto_usd
            total_usd += monto_usd
        else:
            summary[key]["total"] += monto_cs
            total_cs += monto_cs

    summary_rows = sorted(summary.values(), key=lambda row: (row["banco"], row["moneda"]))
    summary_grouped = {}
    for row in summary_rows:
        summary_grouped.setdefault(row["banco"], []).append(row)
    summary_grouped_rows = [
        {"banco": banco, "rows": rows} for banco, rows in summary_grouped.items()
    ]

    return request.app.state.templates.TemplateResponse(
        "sales_depositos.html",
        {
            "request": request,
            "user": user,
            "branch": branch,
            "bodega": bodega,
            "vendedores": vendedores,
            "bancos": bancos,
            "cuentas": cuentas,
            "depositos": depositos,
            "summary_rows": summary_rows,
            "summary_grouped_rows": summary_grouped_rows,
            "total_cs": float(total_cs),
            "total_usd": float(total_usd),
            "today": today_value.isoformat(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "vendedor_q": vendedor_q or "",
            "banco_q": banco_q or "",
            "moneda_q": moneda_q or "",
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/sales/ventas-caliente")
def sales_ventas_caliente(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.sales")
    today = local_today()
    first_day = date(today.year, today.month, 1)
    next_month = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= today)
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    tasa_default = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

    def to_usd(factura: VentaFactura) -> Decimal:
        moneda = factura.moneda or "CS"
        if moneda == "USD":
            return Decimal(str(factura.total_usd or 0))
        tasa = Decimal(str(factura.tasa_cambio or 0)) or tasa_default
        if not tasa:
            return Decimal("0")
        return Decimal(str(factura.total_cs or 0)) / tasa

    branches = db.query(Branch).all()
    branch_map = {b.code.lower(): b for b in branches if b.code}
    central = branch_map.get("central")
    esteli = branch_map.get("esteli")

    base_query = (
        db.query(VentaFactura)
        .join(Bodega, VentaFactura.bodega_id == Bodega.id)
        .join(Branch, Bodega.branch_id == Branch.id)
        .filter(VentaFactura.estado != "ANULADA")
    )

    def total_branch(branch: Optional[Branch]) -> Decimal:
        if not branch:
            return Decimal("0")
        rows = (
            base_query.filter(Branch.id == branch.id)
            .filter(VentaFactura.fecha >= first_day, VentaFactura.fecha < next_month)
            .all()
        )
        total = sum(to_usd(row) for row in rows)
        return Decimal(str(total))

    def total_branch_day(branch: Optional[Branch]) -> Decimal:
        if not branch:
            return Decimal("0")
        rows = (
            base_query.filter(Branch.id == branch.id)
            .filter(func.date(VentaFactura.fecha) == today)
            .all()
        )
        total = sum(to_usd(row) for row in rows)
        return Decimal(str(total))

    total_central = total_branch(central)
    total_esteli = total_branch(esteli)
    total_all = total_central + total_esteli
    total_central_day = total_branch_day(central)
    total_esteli_day = total_branch_day(esteli)
    total_day_all = total_central_day + total_esteli_day

    monthly_rows = (
        base_query.filter(VentaFactura.fecha >= first_day, VentaFactura.fecha < next_month).all()
    )
    totals_by_day = {}
    for row in monthly_rows:
        day = row.fecha.date() if isinstance(row.fecha, datetime) else row.fecha
        totals_by_day[day] = totals_by_day.get(day, Decimal("0")) + to_usd(row)

    days = []
    cursor = first_day
    while cursor < next_month:
        days.append(cursor)
        cursor += timedelta(days=1)
    chart_points = [
        {
            "label": d.strftime("%d/%m"),
            "value": float(totals_by_day.get(d, Decimal("0"))),
        }
        for d in days
    ]
    month_total = sum(Decimal(str(p["value"])) for p in chart_points)

    return request.app.state.templates.TemplateResponse(
        "sales_caliente.html",
        {
            "request": request,
            "user": user,
            "branch_central": central,
            "branch_esteli": esteli,
            "total_central": float(total_central),
            "total_esteli": float(total_esteli),
            "total_all": float(total_all),
            "total_central_day": float(total_central_day),
            "total_esteli_day": float(total_esteli_day),
            "total_day_all": float(total_day_all),
            "month_total": float(month_total),
            "chart_points": chart_points,
            "month_label": first_day.strftime("%B %Y").capitalize(),
            "tasa": float(tasa_default) if tasa_default else 0,
            "today_label": today.strftime("%d/%m/%Y"),
            "version": settings.UI_VERSION,
        },
    )


@router.get("/reports")
def reports_index(
    request: Request,
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    return request.app.state.templates.TemplateResponse(
        "reports_index.html",
        {
            "request": request,
            "user": user,
            "version": settings.UI_VERSION,
        },
    )


def _sales_report_filters(request: Request):
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    branch_id = request.query_params.get("branch_id")
    vendedor_id = request.query_params.get("vendedor_id")
    producto_q = (request.query_params.get("producto") or "").strip()

    today = local_today()
    start_date = today
    end_date = today
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today
            end_date = today

    if not branch_id:
        branch_id = "all"

    return start_date, end_date, branch_id, vendedor_id, producto_q


def _sales_products_report_filters(request: Request):
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    branch_id = request.query_params.get("branch_id")
    vendedor_id = request.query_params.get("vendedor_id")

    today = local_today()
    start_date = today
    end_date = today
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today
            end_date = today

    if not branch_id:
        branch_id = "all"

    return start_date, end_date, branch_id, vendedor_id


def _build_sales_products_report(
    db: Session,
    start_date: date,
    end_date: date,
    branch_id: str | None,
    vendedor_id: str | None,
):
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

    base_query = (
        db.query(VentaFactura, VentaItem, Producto, Branch)
        .join(VentaItem, VentaItem.factura_id == VentaFactura.id)
        .join(Producto, Producto.id == VentaItem.producto_id)
        .join(Bodega, Bodega.id == VentaFactura.bodega_id, isouter=True)
        .join(Branch, Branch.id == Bodega.branch_id, isouter=True)
        .filter(VentaFactura.fecha >= start_dt, VentaFactura.fecha < end_dt)
        .filter(VentaFactura.estado != "ANULADA")
    )
    if branch_id and branch_id != "all":
        try:
            base_query = base_query.filter(Branch.id == int(branch_id))
        except ValueError:
            pass
    if vendedor_id:
        try:
            base_query = base_query.filter(VentaFactura.vendedor_id == int(vendedor_id))
        except ValueError:
            pass

    rows = base_query.all()
    report_map: dict[int, dict] = {}
    facturas_set = set()

    for factura, item, producto, branch in rows:
        moneda = factura.moneda or "CS"
        tasa_factura = Decimal(str(factura.tasa_cambio or 0))
        if moneda == "CS" and not tasa_factura:
            rate_today = (
                db.query(ExchangeRate)
                .filter(ExchangeRate.effective_date <= factura.fecha)
                .order_by(ExchangeRate.effective_date.desc())
                .first()
            )
            tasa_factura = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

        cantidad = Decimal(str(item.cantidad or 0))
        subtotal_usd = Decimal(str(item.subtotal_usd or 0))
        subtotal_cs = Decimal(str(item.subtotal_cs or 0))
        venta_usd = subtotal_usd if moneda == "USD" else (subtotal_cs / tasa_factura if tasa_factura else Decimal("0"))
        venta_cs = subtotal_cs if moneda == "CS" else (subtotal_usd * tasa_factura if tasa_factura else Decimal("0"))

        costo_cs_unit = Decimal(str(producto.costo_producto or 0))
        costo_cs = costo_cs_unit * cantidad
        tasa_producto = Decimal(str(producto.tasa_cambio or 0))
        if not tasa_producto:
            tasa_producto = tasa_factura
        if not tasa_producto:
            rate_today = (
                db.query(ExchangeRate)
                .filter(ExchangeRate.effective_date <= factura.fecha)
                .order_by(ExchangeRate.effective_date.desc())
                .first()
            )
            tasa_producto = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")
        costo_usd = costo_cs / tasa_producto if tasa_producto else Decimal("0")

        if producto.id not in report_map:
            report_map[producto.id] = {
                "codigo": producto.cod_producto,
                "producto": producto.descripcion,
                "cantidad": Decimal("0"),
                "venta_usd": Decimal("0"),
                "venta_cs": Decimal("0"),
                "costo_usd": Decimal("0"),
                "costo_cs": Decimal("0"),
            }
        report_row = report_map[producto.id]
        report_row["cantidad"] += cantidad
        report_row["venta_usd"] += venta_usd
        report_row["venta_cs"] += venta_cs
        report_row["costo_usd"] += costo_usd
        report_row["costo_cs"] += costo_cs
        facturas_set.add(factura.id)

    report_rows = []
    total_qty = Decimal("0")
    total_usd = Decimal("0")
    total_cs = Decimal("0")
    total_cost_usd = Decimal("0")
    total_cost_cs = Decimal("0")
    for row in report_map.values():
        total_qty += row["cantidad"]
        total_usd += row["venta_usd"]
        total_cs += row["venta_cs"]
        total_cost_usd += row["costo_usd"]
        total_cost_cs += row["costo_cs"]
        report_rows.append(
            {
                "codigo": row["codigo"],
                "producto": row["producto"],
                "cantidad": float(row["cantidad"]),
                "venta_usd": float(row["venta_usd"]),
                "venta_cs": float(row["venta_cs"]),
                "costo_usd": float(row["costo_usd"]),
                "costo_cs": float(row["costo_cs"]),
                "margen_usd": float(row["venta_usd"] - row["costo_usd"]),
            }
        )

    report_rows.sort(key=lambda r: r["venta_usd"], reverse=True)
    total_facturas = len(facturas_set)
    return (
        report_rows,
        total_qty,
        total_usd,
        total_cs,
        total_cost_usd,
        total_cost_cs,
        total_facturas,
    )


def _depositos_report_filters(request: Request):
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    branch_id = request.query_params.get("branch_id")

    today = local_today()
    start_date = today
    end_date = today
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today
            end_date = today

    if not branch_id:
        branch_id = "all"

    return start_date, end_date, branch_id


def _depositos_report_query(
    db: Session,
    start_date: date,
    end_date: date,
    branch_id: str | None,
):
    query = (
        db.query(DepositoCliente)
        .join(Bodega, DepositoCliente.bodega_id == Bodega.id, isouter=True)
        .join(Branch, Bodega.branch_id == Branch.id, isouter=True)
        .filter(DepositoCliente.fecha.between(start_date, end_date))
    )
    if branch_id and branch_id != "all":
        try:
            query = query.filter(Branch.id == int(branch_id))
        except ValueError:
            pass
    return query


def _kardex_report_filters(request: Request):
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    branch_id = request.query_params.get("branch_id")
    producto_q = (request.query_params.get("producto") or "").strip()

    today = local_today()
    start_date = today
    end_date = today
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today
            end_date = today

    if not branch_id:
        branch_id = "all"

    return start_date, end_date, branch_id, producto_q


def _kardex_cost_unit_usd(
    db: Session,
    producto: Producto,
    tasa_fallback: Decimal,
) -> Decimal:
    costo_cs = Decimal(str(producto.costo_producto or 0))
    tasa = Decimal(str(producto.tasa_cambio or 0)) or tasa_fallback
    if not tasa:
        rate_today = (
            db.query(ExchangeRate)
            .filter(ExchangeRate.effective_date <= local_today())
            .order_by(ExchangeRate.effective_date.desc())
            .first()
        )
        tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")
    return costo_cs / tasa if tasa else Decimal("0")


def _build_kardex_movements(
    db: Session,
    start_date: date,
    end_date: date,
    branch_id: str | None,
    producto_q: str,
):
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

    producto_filter = None
    if producto_q:
        like = f"%{producto_q.lower()}%"
        producto_filter = or_(
            func.lower(Producto.cod_producto).like(like),
            func.lower(Producto.descripcion).like(like),
        )

    branch_filter = None
    if branch_id and branch_id != "all":
        try:
            branch_filter = int(branch_id)
        except ValueError:
            branch_filter = None

    movimientos = []

    ingresos_q = (
        db.query(IngresoInventario, IngresoItem, Producto, Bodega, Branch)
        .join(IngresoItem, IngresoItem.ingreso_id == IngresoInventario.id)
        .join(Producto, Producto.id == IngresoItem.producto_id)
        .join(Bodega, Bodega.id == IngresoInventario.bodega_id)
        .join(Branch, Branch.id == Bodega.branch_id)
        .filter(IngresoInventario.fecha >= start_date, IngresoInventario.fecha <= end_date)
    )
    if branch_filter:
        ingresos_q = ingresos_q.filter(Branch.id == branch_filter)
    if producto_filter is not None:
        ingresos_q = ingresos_q.filter(producto_filter)

    for ingreso, item, producto, bodega, branch in ingresos_q.all():
        cantidad = Decimal(str(item.cantidad or 0))
        costo_unit_cs = Decimal(str(item.costo_unitario_cs or 0))
        costo_unit_usd = Decimal(str(item.costo_unitario_usd or 0))
        movimientos.append(
            {
                "fecha": ingreso.fecha,
                "tipo": "Ingreso",
                "branch": branch.name if branch else "-",
                "bodega": bodega.name if bodega else "-",
                "producto_id": producto.id,
                "codigo": producto.cod_producto,
                "descripcion": producto.descripcion,
                "cantidad": cantidad,
                "costo_unit_cs": costo_unit_cs,
                "costo_unit_usd": costo_unit_usd,
            }
        )

    egresos_q = (
        db.query(EgresoInventario, EgresoItem, Producto, Bodega, Branch)
        .join(EgresoItem, EgresoItem.egreso_id == EgresoInventario.id)
        .join(Producto, Producto.id == EgresoItem.producto_id)
        .join(Bodega, Bodega.id == EgresoInventario.bodega_id)
        .join(Branch, Branch.id == Bodega.branch_id)
        .filter(EgresoInventario.fecha >= start_date, EgresoInventario.fecha <= end_date)
    )
    if branch_filter:
        egresos_q = egresos_q.filter(Branch.id == branch_filter)
    if producto_filter is not None:
        egresos_q = egresos_q.filter(producto_filter)

    for egreso, item, producto, bodega, branch in egresos_q.all():
        cantidad = Decimal(str(item.cantidad or 0)) * Decimal("-1")
        costo_unit_cs = Decimal(str(item.costo_unitario_cs or 0))
        costo_unit_usd = Decimal(str(item.costo_unitario_usd or 0))
        movimientos.append(
            {
                "fecha": egreso.fecha,
                "tipo": "Egreso",
                "branch": branch.name if branch else "-",
                "bodega": bodega.name if bodega else "-",
                "producto_id": producto.id,
                "codigo": producto.cod_producto,
                "descripcion": producto.descripcion,
                "cantidad": cantidad,
                "costo_unit_cs": costo_unit_cs,
                "costo_unit_usd": costo_unit_usd,
            }
        )

    ventas_q = (
        db.query(VentaFactura, VentaItem, Producto, Bodega, Branch)
        .join(VentaItem, VentaItem.factura_id == VentaFactura.id)
        .join(Producto, Producto.id == VentaItem.producto_id)
        .join(Bodega, Bodega.id == VentaFactura.bodega_id, isouter=True)
        .join(Branch, Branch.id == Bodega.branch_id, isouter=True)
        .filter(VentaFactura.fecha >= start_dt, VentaFactura.fecha < end_dt)
        .filter(VentaFactura.estado != "ANULADA")
    )
    if branch_filter:
        ventas_q = ventas_q.filter(Branch.id == branch_filter)
    if producto_filter is not None:
        ventas_q = ventas_q.filter(producto_filter)

    for factura, item, producto, bodega, branch in ventas_q.all():
        cantidad = Decimal(str(item.cantidad or 0)) * Decimal("-1")
        tasa_factura = Decimal(str(factura.tasa_cambio or 0))
        costo_unit_usd = _kardex_cost_unit_usd(db, producto, tasa_factura)
        costo_unit_cs = Decimal(str(producto.costo_producto or 0))
        movimientos.append(
            {
                "fecha": factura.fecha.date() if isinstance(factura.fecha, datetime) else factura.fecha,
                "tipo": "Venta",
                "branch": branch.name if branch else "-",
                "bodega": bodega.name if bodega else "-",
                "producto_id": producto.id,
                "codigo": producto.cod_producto,
                "descripcion": producto.descripcion,
                "cantidad": cantidad,
                "costo_unit_cs": costo_unit_cs,
                "costo_unit_usd": costo_unit_usd,
            }
        )

    movimientos.sort(key=lambda row: (row["fecha"], row["tipo"]))

    saldos = {}
    rows = []
    for mov in movimientos:
        key = (mov["producto_id"], mov["bodega"])
        saldo = saldos.get(key, Decimal("0"))
        saldo += mov["cantidad"]
        saldos[key] = saldo
        costo_unit_cs = mov["costo_unit_cs"]
        costo_unit_usd = mov["costo_unit_usd"]
        rows.append(
            {
                **mov,
                "saldo": saldo,
                "costo_total_cs": saldo * costo_unit_cs,
                "costo_total_usd": saldo * costo_unit_usd,
            }
        )

    resumen = {
        "movimientos": len(rows),
        "dias": (end_date - start_date).days + 1,
        "total_ingresos": sum((r["cantidad"] for r in rows if r["tipo"] == "Ingreso"), Decimal("0")),
        "total_egresos": sum((abs(r["cantidad"]) for r in rows if r["tipo"] == "Egreso"), Decimal("0")),
        "total_ventas": sum((abs(r["cantidad"]) for r in rows if r["tipo"] == "Venta"), Decimal("0")),
    }

    return rows, resumen


def _build_sales_report_rows(
    db: Session,
    user: User,
    start_date: date,
    end_date: date,
    branch_id: str | None,
    vendedor_id: str | None,
    producto_q: str,
):
    _, bodega_user = _resolve_branch_bodega(db, user)
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

    query = (
        db.query(VentaFactura, VentaItem, Producto, Cliente, Vendedor, Branch)
        .join(VentaItem, VentaItem.factura_id == VentaFactura.id)
        .join(Producto, Producto.id == VentaItem.producto_id)
        .join(Bodega, Bodega.id == VentaFactura.bodega_id, isouter=True)
        .join(Branch, Branch.id == Bodega.branch_id, isouter=True)
        .join(Cliente, Cliente.id == VentaFactura.cliente_id, isouter=True)
        .join(Vendedor, Vendedor.id == VentaFactura.vendedor_id, isouter=True)
        .filter(VentaFactura.fecha >= start_dt, VentaFactura.fecha < end_dt)
    )
    if branch_id and branch_id != "all":
        try:
            query = query.filter(Branch.id == int(branch_id))
        except ValueError:
            pass
    if vendedor_id:
        try:
            query = query.filter(VentaFactura.vendedor_id == int(vendedor_id))
        except ValueError:
            pass
    if producto_q:
        query = query.filter(
            or_(
                func.lower(Producto.cod_producto).like(f"%{producto_q.lower()}%"),
                func.lower(Producto.descripcion).like(f"%{producto_q.lower()}%"),
            )
        )

    rows = query.order_by(VentaFactura.secuencia.asc(), VentaFactura.id.asc()).all()
    report_rows = []
    total_usd = Decimal("0")
    total_cs = Decimal("0")
    facturas_set = set()
    total_items = Decimal("0")
    for factura, item, producto, cliente, vendedor, branch in rows:
        moneda = factura.moneda or "CS"
        tasa = Decimal(str(factura.tasa_cambio or 0))
        if moneda == "CS" and not tasa:
            rate_today = (
                db.query(ExchangeRate)
                .filter(ExchangeRate.effective_date <= factura.fecha)
                .order_by(ExchangeRate.effective_date.desc())
                .first()
            )
            tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")
        subtotal = Decimal(str(item.subtotal_usd or 0)) if moneda == "USD" else Decimal(str(item.subtotal_cs or 0))
        subtotal_usd = subtotal if moneda == "USD" else (subtotal / tasa if tasa else Decimal("0"))
        subtotal_cs = subtotal if moneda == "CS" else (subtotal * tasa if tasa else Decimal("0"))
        precio_unit = (
            Decimal(str(item.precio_unitario_usd or 0))
            if moneda == "USD"
            else Decimal(str(item.precio_unitario_cs or 0))
        )
        total_factura = (
            Decimal(str(factura.total_usd or 0)) if moneda == "USD" else Decimal(str(factura.total_cs or 0))
        )
        total_factura_usd = total_factura if moneda == "USD" else (total_factura / tasa if tasa else Decimal("0"))
        total_factura_cs = total_factura if moneda == "CS" else (total_factura * tasa if tasa else Decimal("0"))
        is_anulada = factura.estado == "ANULADA"
        if not is_anulada:
            total_usd += subtotal_usd
            total_cs += subtotal_cs
            facturas_set.add(factura.id)
            total_items += Decimal(str(item.cantidad or 0))
        report_rows.append(
            {
                "fecha": factura.fecha.strftime("%d/%m/%Y") if factura.fecha else "",
                "factura": factura.numero,
                "cliente": cliente.nombre if cliente else "Consumidor final",
                "vendedor": vendedor.nombre if vendedor else "-",
                "sucursal": branch.name if branch else "-",
                "producto": f"{producto.cod_producto} - {producto.descripcion}",
                "cantidad": float(item.cantidad or 0),
                "moneda": moneda,
                "precio_usd": float(precio_unit if moneda == "USD" else (precio_unit / tasa if tasa else Decimal("0"))),
                "precio_cs": float(precio_unit if moneda == "CS" else (precio_unit * tasa if tasa else Decimal("0"))),
                "subtotal_usd": float(subtotal_usd),
                "subtotal_cs": float(subtotal_cs),
                "total_factura_usd": float(total_factura_usd),
                "total_factura_cs": float(total_factura_cs),
                "anulada": is_anulada,
                "factura_id": factura.id,
            }
        )
    return report_rows, total_usd, total_cs, len(facturas_set), float(total_items)


@router.get("/reports/ventas")
def report_sales_detailed(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    start_date, end_date, branch_id, vendedor_id, producto_q = _sales_report_filters(request)
    report_rows, total_usd, total_cs, total_facturas, total_items = _build_sales_report_rows(
        db,
        user,
        start_date,
        end_date,
        branch_id,
        vendedor_id,
        producto_q,
    )

    branches = db.query(Branch).order_by(Branch.name).all()
    _, bodega = _resolve_branch_bodega(db, user)
    vendedores = _vendedores_for_bodega(db, bodega)

    return request.app.state.templates.TemplateResponse(
        "report_sales_detailed.html",
        {
            "request": request,
            "user": user,
            "rows": report_rows,
            "branches": branches,
            "vendedores": vendedores,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "selected_branch": branch_id or "",
            "selected_vendedor": vendedor_id or "",
            "producto_q": producto_q,
            "total_usd": float(total_usd),
            "total_cs": float(total_cs),
            "total_facturas": total_facturas,
            "total_items": total_items,
            "version": settings.UI_VERSION,
        },
      )


@router.get("/reports/depositos")
def report_depositos(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    start_date, end_date, branch_id = _depositos_report_filters(request)
    depositos = (
        _depositos_report_query(db, start_date, end_date, branch_id)
        .order_by(DepositoCliente.fecha.desc(), DepositoCliente.id.desc())
        .all()
    )
    branches = db.query(Branch).order_by(Branch.name).all()

    total_cs = Decimal("0")
    total_usd = Decimal("0")
    for dep in depositos:
        if dep.moneda == "USD":
            total_usd += Decimal(str(dep.monto_usd or 0))
        else:
            total_cs += Decimal(str(dep.monto_cs or 0))

    return request.app.state.templates.TemplateResponse(
        "report_depositos.html",
        {
            "request": request,
            "user": user,
            "depositos": depositos,
            "branches": branches,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "selected_branch": branch_id or "",
            "total_cs": float(total_cs),
            "total_usd": float(total_usd),
            "version": settings.UI_VERSION,
        },
    )


@router.get("/reports/depositos/export")
def report_depositos_export(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    start_date, end_date, branch_id = _depositos_report_filters(request)
    depositos = (
        _depositos_report_query(db, start_date, end_date, branch_id)
        .order_by(DepositoCliente.banco_id, DepositoCliente.fecha)
        .all()
    )
    branches = db.query(Branch).order_by(Branch.name).all()
    selected_branch = None
    if branch_id and branch_id != "all":
        try:
            selected_branch = next((b for b in branches if b.id == int(branch_id)), None)
        except ValueError:
            selected_branch = None

    grouped, total_cs, total_usd = _depositos_grouped(depositos)
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    rate = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")
    total_usd_equiv = total_usd + (total_cs / rate if rate else Decimal("0"))

    buffer = io.BytesIO()
    width = 380
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import portrait
    from reportlab.lib.units import mm

    c = canvas.Canvas(buffer, pagesize=portrait((width, 600)))
    y = 560
    logo_path = Path(__file__).resolve().parent.parent / "static" / "logo_hollywood.png"
    if logo_path.exists():
        c.drawImage(str(logo_path), 24, y - 40, width=90, height=40, mask="auto")
    c.setFont("Times-Bold", 11)
    c.drawString(120, y - 8, "Informe de Depositos, Transferencias y")
    c.drawString(120, y - 24, "Tarjetas Pacas Hollywood")
    y -= 50
    c.setFont("Times-Roman", 9)
    c.setFillColor(colors.HexColor("#4b5563"))
    if selected_branch:
        c.drawString(24, y, f"Sucursal: {selected_branch.name}")
        y -= 14
    c.drawString(24, y, f"Rango: {start_date} a {end_date}")
    y -= 14
    c.drawString(24, y, f"Tasa: {rate_today.rate if rate_today else 'N/D'}")
    y -= 14
    c.setFillColor(colors.black)
    c.line(24, y, width - 24, y)
    y -= 12

    grouped_map = {}
    for dep in depositos:
        banco_name = dep.banco.nombre if dep.banco else "-"
        if len(banco_name) > 12:
            banco_name = banco_name[:12] + "…"
        grouped_map.setdefault(dep.banco_id, {"banco": banco_name, "rows": []})
        grouped_map[dep.banco_id]["rows"].append(dep)
    grouped_list = sorted(grouped_map.values(), key=lambda row: row["banco"])

    total_count = 0
    for group in grouped_list:
        if y < 90:
            c.showPage()
            y = 560
        c.setFillColor(colors.HexColor("#1e3a8a"))
        c.roundRect(24, y - 6, width - 48, 16, 4, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Times-Bold", 9)
        c.drawString(30, y - 2, group["banco"])
        c.setFillColor(colors.black)
        y -= 20

        c.setFont("Times-Bold", 8)
        c.drawString(24, y, "Fecha")
        c.drawString(95, y, "Banco")
        c.drawRightString(210, y, "Monto Cordobas")
        c.drawRightString(300, y, "Monto Dolares")
        c.drawString(310, y, "Vendedor")
        y -= 12
        c.setFont("Times-Roman", 8)

        subtotal_cs = Decimal("0")
        subtotal_usd = Decimal("0")
        for dep in group["rows"]:
            if y < 70:
                c.showPage()
                y = 560
            total_count += 1
            fecha_text = dep.fecha.strftime("%d/%m/%Y") if dep.fecha else ""
            vendedor_text = dep.vendedor.nombre if dep.vendedor else "-"
            banco_text = dep.banco.nombre if dep.banco else "-"
            c.drawString(24, y, fecha_text)
            c.drawString(95, y, banco_text[:10])
            if dep.moneda == "USD":
                monto_usd = Decimal(str(dep.monto_usd or 0))
                subtotal_usd += monto_usd
                c.setFillColor(colors.HexColor("#16a34a"))
                c.drawRightString(210, y, "C$ 0.00")
                c.drawRightString(300, y, f"$ {monto_usd:,.2f}")
                c.setFillColor(colors.black)
            else:
                monto_cs = Decimal(str(dep.monto_cs or 0))
                subtotal_cs += monto_cs
                c.setFillColor(colors.HexColor("#1d4ed8"))
                c.drawRightString(210, y, f"C$ {monto_cs:,.2f}")
                c.drawRightString(300, y, "$ 0.00")
                c.setFillColor(colors.black)
            c.drawString(310, y, vendedor_text[:18])
            y -= 12

        y -= 6
        c.setFont("Times-Bold", 9)
        c.drawString(30, y, "Total depositos :")
        c.setFillColor(colors.HexColor("#1d4ed8"))
        c.drawString(140, y, f"C$ {subtotal_cs:,.2f}")
        c.setFillColor(colors.HexColor("#16a34a"))
        c.drawRightString(width - 24, y, f"$ {subtotal_usd:,.2f}")
        c.setFillColor(colors.black)
        y -= 18
        c.line(24, y, width - 24, y)
        y -= 16

    c.setFont("Times-Bold", 10)
    c.drawString(24, y, "Totales depositos :")
    c.setFillColor(colors.HexColor("#1d4ed8"))
    c.drawRightString(width - 120, y, f"C$ {total_cs:,.2f}")
    c.setFillColor(colors.HexColor("#16a34a"))
    c.drawRightString(width - 24, y, f"$ {total_usd:,.2f}")
    c.setFillColor(colors.black)
    y -= 18
    c.setFont("Times-Bold", 10)
    c.drawString(24, y, "Totales depositos Dolarizado")
    c.setFillColor(colors.HexColor("#16a34a"))
    c.drawRightString(width - 24, y, f"$ {total_usd_equiv:,.2f}")
    c.setFillColor(colors.black)
    y -= 14
    c.setFont("Times-Roman", 9)
    c.drawRightString(width - 24, y, f"Cantidad de DP: {total_count}")

    c.showPage()
    c.save()
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "inline; filename=depositos_reporte.pdf"},
    )


@router.get("/reports/ventas-productos")
def report_sales_products(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    start_date, end_date, branch_id, vendedor_id = _sales_products_report_filters(request)
    (
        report_rows,
        total_qty,
        total_usd,
        total_cs,
        total_cost_usd,
        total_cost_cs,
        total_facturas,
    ) = _build_sales_products_report(db, start_date, end_date, branch_id, vendedor_id)

    branches = db.query(Branch).order_by(Branch.name).all()
    _, bodega = _resolve_branch_bodega(db, user)
    vendedores = _vendedores_for_bodega(db, bodega)

    return request.app.state.templates.TemplateResponse(
        "report_sales_products.html",
        {
            "request": request,
            "user": user,
            "rows": report_rows,
            "branches": branches,
            "vendedores": vendedores,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "selected_branch": branch_id or "",
            "selected_vendedor": vendedor_id or "",
            "total_qty": float(total_qty),
            "total_usd": float(total_usd),
            "total_cs": float(total_cs),
            "total_cost_usd": float(total_cost_usd),
            "total_cost_cs": float(total_cost_cs),
            "total_facturas": total_facturas,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/reports/kardex")
def report_kardex(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    start_date, end_date, branch_id, producto_q = _kardex_report_filters(request)
    rows, resumen = _build_kardex_movements(db, start_date, end_date, branch_id, producto_q)
    branches = db.query(Branch).order_by(Branch.name).all()

    return request.app.state.templates.TemplateResponse(
        "report_kardex.html",
        {
            "request": request,
            "user": user,
            "rows": rows,
            "resumen": resumen,
            "branches": branches,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "selected_branch": branch_id or "",
            "producto_q": producto_q,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/reports/kardex/export")
def report_kardex_export(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    start_date, end_date, branch_id, producto_q = _kardex_report_filters(request)
    rows, resumen = _build_kardex_movements(db, start_date, end_date, branch_id, producto_q)
    branches = db.query(Branch).order_by(Branch.name).all()
    selected_branch = None
    if branch_id and branch_id != "all":
        try:
            selected_branch = next((b for b in branches if b.id == int(branch_id)), None)
        except ValueError:
            selected_branch = None

    buffer = io.BytesIO()
    width = 380
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import portrait

    c = canvas.Canvas(buffer, pagesize=portrait((width, 700)))
    y = 660
    logo_path = Path(__file__).resolve().parent.parent / "static" / "logo_hollywood.png"
    if logo_path.exists():
        c.drawImage(str(logo_path), 24, y - 40, width=90, height=40, mask="auto")
    c.setFont("Times-Bold", 11)
    c.drawString(120, y - 8, "Reporte Kardex por producto")
    c.drawString(120, y - 24, "Movimientos de inventario")
    y -= 50
    c.setFont("Times-Roman", 9)
    c.setFillColor(colors.HexColor("#4b5563"))
    if selected_branch:
        c.drawString(24, y, f"Sucursal: {selected_branch.name}")
        y -= 14
    c.drawString(24, y, f"Rango: {start_date} a {end_date}")
    y -= 14
    c.drawString(24, y, f"Producto: {producto_q or 'Todos'}")
    y -= 14
    c.drawString(24, y, f"Dias: {resumen['dias']}")
    y -= 14
    c.drawString(24, y, f"Ingresos: {resumen['total_ingresos']}")
    y -= 14
    c.drawString(24, y, f"Egresos: {resumen['total_egresos']}")
    y -= 14
    c.drawString(24, y, f"Ventas: {resumen['total_ventas']}")
    y -= 14
    c.setFillColor(colors.black)
    c.line(24, y, width - 24, y)
    y -= 12

    c.setFont("Times-Bold", 8)
    c.drawString(24, y, "Fecha")
    c.drawString(70, y, "Sucursal/Bodega")
    c.drawString(170, y, "Producto")
    c.drawRightString(230, y, "Cant")
    c.drawRightString(270, y, "Saldo")
    c.drawRightString(330, y, "Costo Unit")
    c.drawRightString(width - 24, y, "Costo Total")
    y -= 12
    c.setFont("Times-Roman", 8)

    for row in rows:
        if y < 70:
            c.showPage()
            y = 660
        fecha_text = row["fecha"].strftime("%d/%m/%Y") if row["fecha"] else ""
        sucursal_text = f"{row['branch']} / {row['bodega']}"
        prod_text = f"{row['codigo']} {row['descripcion'][:14]}"
        c.drawString(24, y, fecha_text)
        c.drawString(70, y, sucursal_text[:18])
        c.drawString(170, y, prod_text)
        c.drawRightString(230, y, f"{row['cantidad']:.2f}")
        c.drawRightString(270, y, f"{row['saldo']:.2f}")
        c.drawRightString(330, y, f"{row['costo_unit_cs']:.2f}")
        c.drawRightString(width - 24, y, f"{row['costo_total_cs']:.2f}")
        y -= 12

    c.showPage()
    c.save()
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "inline; filename=kardex.pdf"},
    )


@router.get("/reports/ventas/export")
def report_sales_export(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.reports")
    start_date, end_date, branch_id, vendedor_id, producto_q = _sales_report_filters(request)
    report_rows, total_usd, total_cs, total_facturas, total_items = _build_sales_report_rows(
        db,
        user,
        start_date,
        end_date,
        branch_id,
        vendedor_id,
        producto_q,
    )

    wb = Workbook()
    ws = wb.active
    ws.title = "Ventas Detallado"
    headers = [
        "Fecha",
        "Factura",
        "Cliente",
        "Vendedor",
        "Sucursal",
        "Producto",
        "Cantidad",
        "Moneda",
        "Precio",
        "Subtotal",
        "Total Factura",
        "Estado",
    ]
    ws.append(headers)
    header_font = Font(bold=True)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    for row in report_rows:
        estado = "ANULADA" if row["anulada"] else "ACTIVA"
        ws.append(
            [
                row["fecha"],
                row["factura"],
                row["cliente"],
                row["vendedor"],
                row["sucursal"],
                row["producto"],
                row["cantidad"],
                row["moneda"],
                row["precio_usd"] if row["moneda"] == "USD" else row["precio_cs"],
                row["subtotal_usd"] if row["moneda"] == "USD" else row["subtotal_cs"],
                row["total_factura_usd"] if row["moneda"] == "USD" else row["total_factura_cs"],
                estado,
            ]
        )

    ws.append([])
    ws.append(["Total USD", float(total_usd)])
    ws.append(["Total C$", float(total_cs)])
    ws.append(["Facturas", total_facturas])
    ws.append(["Items", total_items])
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)
    headers = {"Content-Disposition": "attachment; filename=ventas_detallado.xlsx"}
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


def _depositos_filters(request: Request):
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    vendedor_q = request.query_params.get("vendedor_id")
    banco_q = request.query_params.get("banco_id")
    moneda_q = request.query_params.get("moneda")
    today_value = local_today()
    start_date = today_value
    end_date = today_value
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today_value
            end_date = today_value
    return start_date, end_date, vendedor_q, banco_q, moneda_q


def _depositos_grouped(depositos):
    summary = {}
    total_cs = Decimal("0")
    total_usd = Decimal("0")
    for dep in depositos:
        key = dep.banco_id
        if key not in summary:
            summary[key] = {
                "banco": dep.banco.nombre if dep.banco else "-",
                "total_cs": Decimal("0"),
                "total_usd": Decimal("0"),
            }
        monto_cs = Decimal(str(dep.monto_cs or 0))
        monto_usd = Decimal(str(dep.monto_usd or 0))
        if dep.moneda == "USD":
            summary[key]["total_usd"] += monto_usd
            total_usd += monto_usd
        else:
            summary[key]["total_cs"] += monto_cs
            total_cs += monto_cs
    grouped = sorted(summary.values(), key=lambda row: row["banco"])
    return grouped, total_cs, total_usd


@router.get("/sales/depositos/export")
def sales_depositos_export(
    request: Request,
    format: str = "xlsx",
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    start_date, end_date, vendedor_q, banco_q, moneda_q = _depositos_filters(request)
    branch, bodega = _resolve_branch_bodega(db, user)
    vendedores = _vendedores_for_bodega(db, bodega)
    depositos_query = db.query(DepositoCliente)
    if bodega:
        depositos_query = depositos_query.filter(DepositoCliente.bodega_id == bodega.id)
    depositos_query = depositos_query.filter(DepositoCliente.fecha.between(start_date, end_date))
    if vendedor_q:
        depositos_query = depositos_query.filter(DepositoCliente.vendedor_id == int(vendedor_q))
    if banco_q:
        depositos_query = depositos_query.filter(DepositoCliente.banco_id == int(banco_q))
    if moneda_q:
        depositos_query = depositos_query.filter(DepositoCliente.moneda == moneda_q.upper())
    depositos = depositos_query.order_by(DepositoCliente.banco_id, DepositoCliente.fecha).all()

    grouped, total_cs, total_usd = _depositos_grouped(depositos)
    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    rate = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")
    total_usd_equiv = total_usd + (total_cs / rate if rate else Decimal("0"))

    if format.lower() == "pdf":
        buffer = io.BytesIO()
        width = 380
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import portrait
        from reportlab.lib.units import mm

        c = canvas.Canvas(buffer, pagesize=portrait((width, 600)))
        y = 560
        logo_path = Path(__file__).resolve().parent.parent / "static" / "logo_hollywood.png"
        if logo_path.exists():
            c.drawImage(str(logo_path), 24, y - 40, width=90, height=40, mask="auto")
        c.setFont("Times-Bold", 11)
        c.drawString(120, y - 8, "Informe de Depositos, Transferencias y")
        c.drawString(120, y - 24, "Tarjetas Pacas Hollywood")
        y -= 50
        c.setFont("Times-Roman", 9)
        c.setFillColor(colors.HexColor("#4b5563"))
        if branch:
            c.drawString(24, y, f"Sucursal: {branch.name}")
            y -= 14
        c.drawString(24, y, f"Rango: {start_date} a {end_date}")
        y -= 14
        c.drawString(24, y, f"Tasa: {rate_today.rate if rate_today else 'N/D'}")
        y -= 14
        c.setFillColor(colors.black)
        c.line(24, y, width - 24, y)
        y -= 12

        grouped_map = {}
        for dep in depositos:
            banco_name = dep.banco.nombre if dep.banco else "-"
            if len(banco_name) > 12:
                banco_name = banco_name[:12] + "…"
            grouped_map.setdefault(dep.banco_id, {"banco": banco_name, "rows": []})
            grouped_map[dep.banco_id]["rows"].append(dep)
        grouped_list = sorted(grouped_map.values(), key=lambda row: row["banco"])

        total_count = 0
        for group in grouped_list:
            if y < 90:
                c.showPage()
                y = 560
            c.setFillColor(colors.HexColor("#1e3a8a"))
            c.roundRect(24, y - 6, width - 48, 16, 4, fill=1, stroke=0)
            c.setFillColor(colors.white)
            c.setFont("Times-Bold", 9)
            c.drawString(30, y - 2, group["banco"])
            c.setFillColor(colors.black)
            y -= 20

            c.setFont("Times-Bold", 8)
            c.setFillColor(colors.HexColor("#475569"))
            c.drawString(30, y, "Fecha")
            c.drawString(78, y, "Banco")
            c.drawString(140, y, "Monto Cordobas")
            c.drawString(230, y, "Monto Dolares")
            c.drawString(305, y, "Vendedor")
            y -= 8
            c.setFillColor(colors.black)
            c.line(24, y, width - 24, y)
            y -= 10

            subtotal_cs = Decimal("0")
            subtotal_usd = Decimal("0")
            for dep in group["rows"]:
                if y < 90:
                    c.showPage()
                    y = 560
                monto_cs = Decimal(str(dep.monto_cs or 0))
                monto_usd = Decimal(str(dep.monto_usd or 0))
                total_count += 1
                c.setFont("Times-Roman", 7)
                c.setFillColor(colors.black)
                c.drawString(30, y, str(dep.fecha))
                banco_row = dep.banco.nombre if dep.banco else "-"
                if len(banco_row) > 12:
                    banco_row = banco_row[:12] + "…"
                c.drawString(78, y, banco_row)
                c.setFillColor(colors.HexColor("#1d4ed8"))
                display_cs = monto_cs if dep.moneda == "CS" else Decimal("0")
                display_usd = monto_usd if dep.moneda == "USD" else Decimal("0")
                c.drawString(140, y, f"C$ {display_cs:,.2f}")
                c.setFillColor(colors.HexColor("#16a34a"))
                c.drawString(230, y, f"$ {display_usd:,.2f}")
                c.setFillColor(colors.black)
                c.drawString(305, y, dep.vendedor.nombre if dep.vendedor else "-")
                y -= 12
                subtotal_cs += display_cs
                subtotal_usd += display_usd

            y -= 4
            c.setFont("Times-Bold", 8)
            c.setFillColor(colors.HexColor("#475569"))
            c.drawString(30, y, "Total depositos :")
            c.setFillColor(colors.HexColor("#1d4ed8"))
            c.drawString(140, y, f"C$ {subtotal_cs:,.2f}")
            c.setFillColor(colors.HexColor("#16a34a"))
            c.drawString(230, y, f"$ {subtotal_usd:,.2f}")
            c.setFillColor(colors.black)
            y -= 16

        y -= 6
        c.setFont("Times-Bold", 9)
        c.line(24, y, width - 24, y)
        y -= 14
        c.drawString(24, y, "Totales depositos :")
        c.setFillColor(colors.HexColor("#1d4ed8"))
        c.drawRightString(width - 120, y, f"C$ {total_cs:,.2f}")
        c.setFillColor(colors.HexColor("#16a34a"))
        c.drawRightString(width - 24, y, f"$ {total_usd:,.2f}")
        c.setFillColor(colors.black)
        y -= 14
        c.drawString(24, y, "Totales depositos Dolarizado")
        c.setFillColor(colors.HexColor("#16a34a"))
        c.drawRightString(width - 24, y, f"$ {total_usd_equiv:,.2f}")
        c.setFillColor(colors.black)
        y -= 14
        c.drawString(24, y, f"Cantidad de DP: {total_count}")
        c.showPage()
        c.save()
        buffer.seek(0)
        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=depositos.pdf"},
        )

    wb = Workbook()
    ws = wb.active
    ws.title = "Depositos"
    ws["A1"] = "Informe de Depositos"
    ws["A2"] = f"Rango: {start_date} a {end_date}"
    ws["A3"] = f"Tasa: {rate_today.rate if rate_today else 'N/D'}"
    ws["A5"] = "Banco"
    ws["B5"] = "Total C$"
    ws["C5"] = "Total USD"
    for cell in ("A1", "A5", "B5", "C5"):
        ws[cell].font = Font(bold=True)
    row_idx = 6
    for row in grouped:
        ws.cell(row=row_idx, column=1, value=row["banco"])
        ws.cell(row=row_idx, column=2, value=float(row["total_cs"]))
        ws.cell(row=row_idx, column=3, value=float(row["total_usd"]))
        row_idx += 1
    ws.cell(row=row_idx, column=1, value="Totales").font = Font(bold=True)
    ws.cell(row=row_idx, column=2, value=float(total_cs)).font = Font(bold=True)
    ws.cell(row=row_idx, column=3, value=float(total_usd)).font = Font(bold=True)
    row_idx += 1
    ws.cell(row=row_idx, column=1, value="Total USD (convertido)").font = Font(bold=True)
    ws.cell(row=row_idx, column=3, value=float(total_usd_equiv)).font = Font(bold=True)
    for col in range(1, 4):
        ws.column_dimensions[chr(64 + col)].width = 20
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=depositos.xlsx"},
    )


@router.post("/sales/depositos")
async def sales_depositos_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    deposito_id = form.get("deposito_id")
    vendedor_id = form.get("vendedor_id")
    banco_id = form.get("banco_id")
    cuenta_id = form.get("cuenta_id") or None
    fecha_raw = form.get("fecha")
    moneda = (form.get("moneda") or "CS").upper()
    monto_raw = form.get("monto")
    observacion = (form.get("observacion") or "").strip()

    if not vendedor_id or not banco_id or not monto_raw:
        return RedirectResponse("/sales/depositos?error=Datos+incompletos", status_code=303)
    if moneda not in {"CS", "USD"}:
        return RedirectResponse("/sales/depositos?error=Moneda+no+valida", status_code=303)

    def parse_decimal(value: str) -> Decimal:
        raw = re.sub(r"[^0-9.,-]", "", str(value or "0"))
        if "," in raw and "." in raw:
            if raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "," in raw and "." not in raw:
            parts = raw.split(",")
            if len(parts) == 2 and len(parts[1]) == 2:
                raw = raw.replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "." in raw and "," not in raw:
            parts = raw.split(".")
            if len(parts) == 2 and len(parts[1]) == 2:
                raw = raw
            else:
                raw = raw.replace(".", "")
        try:
            return Decimal(raw)
        except Exception:
            return Decimal("0")

    monto = parse_decimal(monto_raw)
    if monto <= 0:
        return RedirectResponse("/sales/depositos?error=Monto+no+valido", status_code=303)

    if fecha_raw:
        try:
            fecha_value = date.fromisoformat(str(fecha_raw).split("T")[0])
        except ValueError:
            fecha_value = local_today()
    else:
        fecha_value = local_today()

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if moneda == "USD" and not rate_today:
        return RedirectResponse("/sales/depositos?error=Tasa+de+cambio+no+configurada", status_code=303)
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

    branch, bodega = _resolve_branch_bodega(db, user)
    if not branch:
        return RedirectResponse("/sales/depositos?error=Usuario+sin+sucursal+asignada", status_code=303)
    if not bodega:
        return RedirectResponse("/sales/depositos?error=Bodega+no+configurada+para+la+sucursal", status_code=303)

    vendedor = db.query(Vendedor).filter(Vendedor.id == int(vendedor_id)).first()
    banco = db.query(Banco).filter(Banco.id == int(banco_id)).first()
    if not vendedor or not banco:
        return RedirectResponse("/sales/depositos?error=Vendedor+o+banco+no+valido", status_code=303)
    cuenta = None
    if cuenta_id:
        cuenta = db.query(CuentaBancaria).filter(CuentaBancaria.id == int(cuenta_id)).first()

    if moneda == "USD":
        monto_usd = monto
        monto_cs = monto * tasa
    else:
        monto_cs = monto
        monto_usd = monto / tasa if tasa else Decimal("0")

    if deposito_id:
        deposito = (
            db.query(DepositoCliente)
            .filter(DepositoCliente.id == int(deposito_id), DepositoCliente.bodega_id == bodega.id)
            .first()
        )
        if not deposito:
            return RedirectResponse("/sales/depositos?error=Deposito+no+encontrado", status_code=303)
        deposito.vendedor_id = vendedor.id
        deposito.banco_id = banco.id
        deposito.cuenta_id = cuenta.id if cuenta else None
        deposito.fecha = fecha_value
        deposito.moneda = moneda
        deposito.tasa_cambio = tasa if tasa else None
        deposito.monto_usd = monto_usd
        deposito.monto_cs = monto_cs
        deposito.observacion = observacion
        deposito.usuario_registro = user.full_name
    else:
        deposito = DepositoCliente(
            branch_id=branch.id,
            bodega_id=bodega.id,
            vendedor_id=vendedor.id,
            banco_id=banco.id,
            cuenta_id=cuenta.id if cuenta else None,
            fecha=fecha_value,
            moneda=moneda,
            tasa_cambio=tasa if tasa else None,
            monto_usd=monto_usd,
            monto_cs=monto_cs,
            observacion=observacion,
            usuario_registro=user.full_name,
        )
        db.add(deposito)
    db.commit()

    msg = "Deposito+actualizado" if deposito_id else "Deposito+registrado"
    return RedirectResponse(f"/sales/depositos?success={msg}", status_code=303)


@router.post("/sales/depositos/{deposito_id}/delete")
def sales_depositos_delete(
    request: Request,
    deposito_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.sales.depositos")
    _, bodega = _resolve_branch_bodega(db, user)
    query = db.query(DepositoCliente).filter(DepositoCliente.id == deposito_id)
    if bodega:
        query = query.filter(DepositoCliente.bodega_id == bodega.id)
    deposito = query.first()
    if not deposito:
        return RedirectResponse("/sales/depositos?error=Deposito+no+encontrado", status_code=303)
    db.delete(deposito)
    db.commit()
    return RedirectResponse("/sales/depositos?success=Deposito+eliminado", status_code=303)


@router.get("/sales/{venta_id}/detail")
def sales_detail(
    venta_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    factura = (
        db.query(VentaFactura)
        .filter(VentaFactura.id == venta_id)
        .first()
    )
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)

    items = []
    for item in factura.items:
        items.append(
            {
                "codigo": item.producto.cod_producto if item.producto else "",
                "descripcion": item.producto.descripcion if item.producto else "",
                "cantidad": float(item.cantidad or 0),
                "precio_usd": float(item.precio_unitario_usd or 0),
                "precio_cs": float(item.precio_unitario_cs or 0),
                "subtotal_usd": float(item.subtotal_usd or 0),
                "subtotal_cs": float(item.subtotal_cs or 0),
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "factura": {
                "id": factura.id,
                "numero": factura.numero,
                "fecha": factura.fecha.isoformat() if factura.fecha else "",
                "hora": factura.created_at.strftime("%H:%M") if factura.created_at else "",
                "cliente": factura.cliente.nombre if factura.cliente else "Consumidor final",
                "vendedor": factura.vendedor.nombre if factura.vendedor else "-",
                "sucursal": factura.bodega.branch.name if factura.bodega and factura.bodega.branch else "-",
                "bodega": factura.bodega.name if factura.bodega else "-",
                "moneda": factura.moneda,
                "total_usd": float(factura.total_usd or 0),
                "total_cs": float(factura.total_cs or 0),
                "items": items,
            },
        }
    )


@router.post("/sales/{venta_id}/reprint")
def sales_reprint(
    venta_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    factura = (
        db.query(VentaFactura)
        .filter(VentaFactura.id == venta_id)
        .first()
    )
    if not factura or not factura.bodega or not factura.bodega.branch:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    return JSONResponse(
        {
            "ok": True,
            "print_url": f"/sales/{factura.id}/ticket/print",
        }
    )


@router.post("/sales/{venta_id}/reversion/request")
async def sales_reversion_request(
    venta_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    motivo = (form.get("motivo") or "").strip()
    if not motivo:
        return JSONResponse({"ok": False, "message": "Motivo requerido"}, status_code=400)

    factura = db.query(VentaFactura).filter(VentaFactura.id == venta_id).first()
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    if factura.estado == "ANULADA":
        return JSONResponse({"ok": False, "message": "Factura ya anulada"}, status_code=400)
    if factura.abonos and len(factura.abonos) > 0:
        return JSONResponse({"ok": False, "message": "No se puede anular con abonos aplicados"}, status_code=400)

    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and factura.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Factura fuera de tu bodega"}, status_code=403)

    config = db.query(EmailConfig).first()
    if not config or not config.active:
        return JSONResponse({"ok": False, "message": "Configura el correo emisor"}, status_code=400)
    recipients = (
        db.query(NotificationRecipient)
        .filter(NotificationRecipient.active.is_(True))
        .all()
    )
    recipient_emails = [r.email for r in recipients]
    if not recipient_emails:
        return JSONResponse({"ok": False, "message": "No hay destinatarios activos"}, status_code=400)

    token = _generate_token()
    expires_at = datetime.utcnow() + timedelta(minutes=10)
    db.add(
        ReversionToken(
            factura_id=factura.id,
            token=token,
            motivo=motivo,
            solicitado_por=user.full_name,
            expires_at=expires_at,
        )
    )
    db.commit()

    branch = factura.bodega.branch if factura.bodega else None
    sucursal = branch.name if branch else "-"
    bodega_name = factura.bodega.name if factura.bodega else "-"
    cliente = factura.cliente.nombre if factura.cliente else "Consumidor final"
    vendedor = factura.vendedor.nombre if factura.vendedor else "-"
    fecha_base = factura.created_at or factura.fecha
    fecha_str = ""
    hora_str = ""
    if fecha_base:
        try:
            fecha_str = fecha_base.strftime("%d/%m/%Y")
            hora_str = fecha_base.strftime("%H:%M")
        except AttributeError:
            fecha_str = str(fecha_base)
    moneda = factura.moneda or "CS"
    total_amount = float(factura.total_cs or 0) if moneda == "CS" else float(factura.total_usd or 0)
    currency_label = "C$" if moneda == "CS" else "$"

    items_html = "".join(
        f"""
        <tr>
          <td style="padding:6px 8px;border-bottom:1px solid #eee;">{item.producto.cod_producto if item.producto else ''}</td>
          <td style="padding:6px 8px;border-bottom:1px solid #eee;">{item.producto.descripcion if item.producto else ''}</td>
          <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right;">{float(item.cantidad or 0):.2f}</td>
          <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right;">{currency_label} {float(item.subtotal_cs if moneda == 'CS' else item.subtotal_usd or 0):,.2f}</td>
        </tr>
        """
        for item in factura.items
    )

    html_body = f"""
    <div style="font-family:Arial,sans-serif;background:#f7f4fb;padding:24px;">
      <div style="max-width:780px;margin:0 auto;background:#ffffff;border-radius:18px;padding:28px;border:1px solid #eadff2;">
        <div style="margin-bottom:16px;">
          <div style="font-size:12px;letter-spacing:2px;text-transform:uppercase;color:#8b7aa8;">Solicitud de anulacion</div>
          <h2 style="margin:6px 0 0;color:#5b2a86;">Sucursal: {sucursal}</h2>
          <div style="margin-top:6px;color:#6a5b86;font-size:14px;">Serie/Factura: <strong>{factura.numero}</strong> | Bodega: {bodega_name}</div>
        </div>

        <div style="background:#f3eefd;border:1px solid #e2d7f5;border-radius:14px;padding:14px;text-align:center;margin-bottom:18px;">
          <div style="font-size:12px;text-transform:uppercase;color:#7b6a98;">Codigo de autorizacion</div>
          <div style="font-size:26px;font-weight:700;color:#5b2a86;letter-spacing:3px;margin-top:6px;">{token}</div>
        </div>

        <div style="background:#faf7ff;border:1px solid #eee3fb;border-radius:12px;padding:12px;margin-bottom:18px;">
          <div style="font-size:12px;text-transform:uppercase;color:#7b6a98;margin-bottom:6px;">Motivo</div>
          <div style="font-size:14px;color:#3b2f52;font-weight:600;">{motivo}</div>
        </div>

        <table style="width:100%;font-size:14px;color:#333;margin-bottom:16px;">
          <tr><td>Cliente:</td><td><strong>{cliente}</strong></td></tr>
          <tr><td>Vendedor:</td><td>{vendedor}</td></tr>
          <tr><td>Usuario:</td><td>{user.full_name}</td></tr>
          <tr><td>Fecha/Hora:</td><td>{fecha_str} {hora_str}</td></tr>
          <tr><td>Total:</td><td><strong>{currency_label} {total_amount:,.2f}</strong></td></tr>
        </table>

        <h3 style="margin:12px 0;color:#5b2a86;">Detalle de items</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead>
            <tr style="background:#f1e8f8;color:#5b2a86;text-align:left;">
              <th style="padding:6px 8px;">Codigo</th>
              <th style="padding:6px 8px;">Descripcion</th>
              <th style="padding:6px 8px;text-align:right;">Cantidad</th>
              <th style="padding:6px 8px;text-align:right;">Subtotal</th>
            </tr>
          </thead>
          <tbody>
            {items_html}
          </tbody>
        </table>
      </div>
    </div>
    """

    send_error = _send_reversion_email(
        subject=f"Reversion {factura.numero}",
        html_body=html_body,
        recipients=recipient_emails,
        sender_email=config.sender_email if config else None,
        sender_name=config.sender_name if config else None,
    )
    if send_error:
        return JSONResponse({"ok": False, "message": send_error}, status_code=500)

    return JSONResponse({"ok": True, "message": "Codigo enviado"})


@router.post("/sales/{venta_id}/reversion/confirm")
async def sales_reversion_confirm(
    venta_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    token = (form.get("token") or "").strip()
    if not token:
        return JSONResponse({"ok": False, "message": "Codigo requerido"}, status_code=400)

    factura = db.query(VentaFactura).filter(VentaFactura.id == venta_id).first()
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    if factura.estado == "ANULADA":
        return JSONResponse({"ok": False, "message": "Factura ya anulada"}, status_code=400)
    if factura.abonos and len(factura.abonos) > 0:
        return JSONResponse({"ok": False, "message": "No se puede anular con abonos aplicados"}, status_code=400)

    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and factura.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Factura fuera de tu bodega"}, status_code=403)

    token_row = (
        db.query(ReversionToken)
        .filter(
            ReversionToken.factura_id == factura.id,
            ReversionToken.token == token,
            ReversionToken.used_at.is_(None),
        )
        .order_by(ReversionToken.created_at.desc())
        .first()
    )
    if not token_row:
        return JSONResponse({"ok": False, "message": "Codigo invalido"}, status_code=400)
    if token_row.expires_at < datetime.utcnow():
        return JSONResponse({"ok": False, "message": "Codigo expirado"}, status_code=400)

    def to_decimal(value: Optional[float]) -> Decimal:
        return Decimal(str(value or 0))

    for item in factura.items:
        if item.producto and item.producto.saldo:
            existencia_actual = to_decimal(item.producto.saldo.existencia)
            item.producto.saldo.existencia = existencia_actual + to_decimal(item.cantidad)

    factura.estado = "ANULADA"
    factura.reversion_motivo = token_row.motivo
    factura.revertida_por = user.full_name
    factura.revertida_at = local_now_naive()
    token_row.used_at = local_now_naive()
    db.commit()
    return JSONResponse({"ok": True, "message": "Factura anulada"})

@router.get("/data")
def data_home(
    request: Request,
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data")
    return request.app.state.templates.TemplateResponse(
        "data.html",
        {
            "request": request,
            "user": user,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/data/pos-print")
def data_pos_print(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(PosPrintSetting).filter(PosPrintSetting.id == int(edit_id)).first()
    items = db.query(PosPrintSetting).order_by(PosPrintSetting.branch_id).all()
    branches = db.query(Branch).order_by(Branch.name).all()
    return request.app.state.templates.TemplateResponse(
        "data_pos_print.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "branches": branches,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.get("/data/notificaciones")
def data_notificaciones(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    config = db.query(EmailConfig).first()
    recipients = db.query(NotificationRecipient).order_by(NotificationRecipient.email).all()
    return request.app.state.templates.TemplateResponse(
        "data_notificaciones.html",
        {
            "request": request,
            "user": user,
            "config": config,
            "recipients": recipients,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/notificaciones/config")
def data_notificaciones_config(
    sender_email: str = Form(...),
    sender_name: Optional[str] = Form(None),
    active: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    sender_email = sender_email.strip()
    if not sender_email:
        return RedirectResponse("/data/notificaciones?error=Correo+requerido", status_code=303)
    config = db.query(EmailConfig).first()
    if not config:
        config = EmailConfig(sender_email=sender_email)
        db.add(config)
    config.sender_email = sender_email
    config.sender_name = sender_name.strip() if sender_name else None
    config.active = active == "on"
    db.commit()
    return RedirectResponse("/data/notificaciones?success=Configuracion+guardada", status_code=303)


@router.post("/data/notificaciones/recipients")
def data_notificaciones_add_recipient(
    email: str = Form(...),
    name: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    email = email.strip().lower()
    if not email:
        return RedirectResponse("/data/notificaciones?error=Correo+requerido", status_code=303)
    exists = db.query(NotificationRecipient).filter(NotificationRecipient.email == email).first()
    if exists:
        return RedirectResponse("/data/notificaciones?error=Correo+ya+existe", status_code=303)
    db.add(NotificationRecipient(email=email, name=name, active=True))
    db.commit()
    return RedirectResponse("/data/notificaciones?success=Destinatario+agregado", status_code=303)


@router.post("/data/notificaciones/recipients/{recipient_id}/update")
def data_notificaciones_update_recipient(
    recipient_id: int,
    email: str = Form(...),
    name: Optional[str] = Form(None),
    active: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    recipient = db.query(NotificationRecipient).filter(NotificationRecipient.id == recipient_id).first()
    if not recipient:
        return RedirectResponse("/data/notificaciones?error=Destinatario+no+existe", status_code=303)
    recipient.email = email.strip().lower()
    recipient.name = name
    recipient.active = active == "on"
    db.commit()
    return RedirectResponse("/data/notificaciones?success=Destinatario+actualizado", status_code=303)


@router.post("/data/pos-print")
def data_pos_print_save(
    branch_id: int = Form(...),
    printer_name: str = Form(...),
    copies: int = Form(1),
    auto_print: Optional[str] = Form(None),
    roc_printer_name: Optional[str] = Form(None),
    roc_copies: Optional[int] = Form(None),
    roc_auto_print: Optional[str] = Form(None),
    cierre_printer_name: Optional[str] = Form(None),
    cierre_copies: Optional[int] = Form(None),
    cierre_auto_print: Optional[str] = Form(None),
    sumatra_path: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    printer_name = printer_name.strip()
    if not printer_name or not branch_id:
        return RedirectResponse("/data/pos-print?error=Datos+incompletos", status_code=303)
    setting = db.query(PosPrintSetting).filter(PosPrintSetting.branch_id == branch_id).first()
    if setting:
        setting.printer_name = printer_name
        setting.copies = max(copies, 1)
        setting.auto_print = auto_print == "on"
        setting.roc_printer_name = roc_printer_name.strip() if roc_printer_name else None
        setting.roc_copies = max(roc_copies or 1, 1) if roc_copies is not None else None
        setting.roc_auto_print = roc_auto_print == "on"
        setting.cierre_printer_name = cierre_printer_name.strip() if cierre_printer_name else None
        setting.cierre_copies = max(cierre_copies or 1, 1) if cierre_copies is not None else None
        setting.cierre_auto_print = cierre_auto_print == "on"
        setting.sumatra_path = sumatra_path.strip() if sumatra_path else None
    else:
        setting = PosPrintSetting(
            branch_id=branch_id,
            printer_name=printer_name,
            copies=max(copies, 1),
            auto_print=auto_print == "on",
            roc_printer_name=roc_printer_name.strip() if roc_printer_name else None,
            roc_copies=max(roc_copies or 1, 1) if roc_copies is not None else None,
            roc_auto_print=roc_auto_print == "on",
            cierre_printer_name=cierre_printer_name.strip() if cierre_printer_name else None,
            cierre_copies=max(cierre_copies or 1, 1) if cierre_copies is not None else None,
            cierre_auto_print=cierre_auto_print == "on",
            sumatra_path=sumatra_path.strip() if sumatra_path else None,
        )
        db.add(setting)
    db.commit()
    return RedirectResponse("/data/pos-print?success=Configuracion+guardada", status_code=303)


@router.post("/data/pos-print/{setting_id}/update")
def data_pos_print_update(
    setting_id: int,
    branch_id: int = Form(...),
    printer_name: str = Form(...),
    copies: int = Form(1),
    auto_print: Optional[str] = Form(None),
    roc_printer_name: Optional[str] = Form(None),
    roc_copies: Optional[int] = Form(None),
    roc_auto_print: Optional[str] = Form(None),
    cierre_printer_name: Optional[str] = Form(None),
    cierre_copies: Optional[int] = Form(None),
    cierre_auto_print: Optional[str] = Form(None),
    sumatra_path: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    setting = db.query(PosPrintSetting).filter(PosPrintSetting.id == setting_id).first()
    if not setting:
        return RedirectResponse("/data/pos-print?error=Configuracion+no+existe", status_code=303)
    setting.branch_id = branch_id
    setting.printer_name = printer_name.strip()
    setting.copies = max(copies, 1)
    setting.auto_print = auto_print == "on"
    setting.roc_printer_name = roc_printer_name.strip() if roc_printer_name else None
    setting.roc_copies = max(roc_copies or 1, 1) if roc_copies is not None else None
    setting.roc_auto_print = roc_auto_print == "on"
    setting.cierre_printer_name = cierre_printer_name.strip() if cierre_printer_name else None
    setting.cierre_copies = max(cierre_copies or 1, 1) if cierre_copies is not None else None
    setting.cierre_auto_print = cierre_auto_print == "on"
    setting.sumatra_path = sumatra_path.strip() if sumatra_path else None
    db.commit()
    return RedirectResponse("/data/pos-print?success=Configuracion+actualizada", status_code=303)


@router.get("/data/vendedores")
def data_vendedores(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(Vendedor).filter(Vendedor.id == int(edit_id)).first()
    items = db.query(Vendedor).order_by(Vendedor.nombre).all()
    bodegas = db.query(Bodega).order_by(Bodega.name).all()
    edit_bodega_ids = []
    edit_default_bodega_id = None
    if edit_item:
        edit_bodega_ids = [asig.bodega_id for asig in (edit_item.assignments or [])]
        default_assignment = next((asig for asig in (edit_item.assignments or []) if asig.is_default), None)
        edit_default_bodega_id = default_assignment.bodega_id if default_assignment else None
    return request.app.state.templates.TemplateResponse(
        "data_vendedores.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "edit_item": edit_item,
            "bodegas": bodegas,
            "edit_bodega_ids": edit_bodega_ids,
            "edit_default_bodega_id": edit_default_bodega_id,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/vendedores")
def data_create_vendedor(
    nombre: str = Form(...),
    telefono: Optional[str] = Form(None),
    bodega_ids: Optional[list[str]] = Form(None),
    default_bodega_id: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        return RedirectResponse("/data/vendedores?error=Nombre+requerido", status_code=303)
    exists = db.query(Vendedor).filter(Vendedor.nombre == nombre).first()
    if exists:
        return RedirectResponse("/data/vendedores?error=Vendedor+ya+existe", status_code=303)
    vendedor = Vendedor(nombre=nombre, telefono=telefono, activo=True)
    db.add(vendedor)
    db.flush()
    selected_ids = {int(b) for b in (bodega_ids or []) if str(b).strip()}
    if default_bodega_id:
        selected_ids.add(int(default_bodega_id))
    for bodega_id in sorted(selected_ids):
        db.add(
            VendedorBodega(
                vendedor_id=vendedor.id,
                bodega_id=bodega_id,
                is_default=default_bodega_id and int(default_bodega_id) == bodega_id,
            )
        )
    db.commit()
    return RedirectResponse("/data/vendedores?success=Vendedor+creado", status_code=303)


@router.post("/data/vendedores/{item_id}/update")
def data_update_vendedor(
    item_id: int,
    nombre: str = Form(...),
    telefono: Optional[str] = Form(None),
    activo: Optional[str] = Form(None),
    bodega_ids: Optional[list[str]] = Form(None),
    default_bodega_id: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    vendedor = db.query(Vendedor).filter(Vendedor.id == item_id).first()
    if not vendedor:
        return RedirectResponse("/data/vendedores?error=Vendedor+no+existe", status_code=303)
    vendedor.nombre = nombre.strip()
    vendedor.telefono = telefono
    vendedor.activo = activo == "on"
    selected_ids = {int(b) for b in (bodega_ids or []) if str(b).strip()}
    if default_bodega_id:
        selected_ids.add(int(default_bodega_id))
    vendedor.assignments.clear()
    for bodega_id in sorted(selected_ids):
        vendedor.assignments.append(
            VendedorBodega(
                vendedor_id=vendedor.id,
                bodega_id=bodega_id,
                is_default=default_bodega_id and int(default_bodega_id) == bodega_id,
            )
        )
    db.commit()
    return RedirectResponse("/data/vendedores?success=Vendedor+actualizado", status_code=303)


@router.get("/data/bancos")
def data_bancos(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(Banco).filter(Banco.id == int(edit_id)).first()
    items = db.query(Banco).order_by(Banco.nombre).all()
    return request.app.state.templates.TemplateResponse(
        "data_bancos.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/bancos")
def data_create_banco(
    nombre: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        return RedirectResponse("/data/bancos?error=Nombre+requerido", status_code=303)
    exists = db.query(Banco).filter(Banco.nombre == nombre).first()
    if exists:
        return RedirectResponse("/data/bancos?error=Banco+ya+existe", status_code=303)
    banco = Banco(nombre=nombre)
    db.add(banco)
    db.commit()
    return RedirectResponse("/data/bancos?success=Banco+creado", status_code=303)


@router.post("/data/bancos/{item_id}/update")
def data_update_banco(
    item_id: int,
    nombre: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    banco = db.query(Banco).filter(Banco.id == item_id).first()
    if not banco:
        return RedirectResponse("/data/bancos?error=Banco+no+existe", status_code=303)
    banco.nombre = nombre.strip()
    db.commit()
    return RedirectResponse("/data/bancos?success=Banco+actualizado", status_code=303)


@router.get("/data/sucursales")
def data_sucursales(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(Branch).filter(Branch.id == int(edit_id)).first()
    items = db.query(Branch).order_by(Branch.name).all()
    return request.app.state.templates.TemplateResponse(
        "data_sucursales.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/sucursales")
def data_create_sucursal(
    code: str = Form(...),
    name: str = Form(...),
    company_name: str = Form(...),
    ruc: str = Form(...),
    telefono: str = Form(...),
    direccion: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    code = code.strip().lower()
    name = name.strip()
    company_name = company_name.strip()
    ruc = ruc.strip()
    telefono = telefono.strip()
    direccion = direccion.strip()
    if not code or not name or not company_name or not ruc or not telefono or not direccion:
        return RedirectResponse("/data/sucursales?error=Datos+incompletos", status_code=303)
    exists = (
        db.query(Branch)
        .filter((func.lower(Branch.code) == code) | (func.lower(Branch.name) == name.lower()))
        .first()
    )
    if exists:
        return RedirectResponse("/data/sucursales?error=Sucursal+ya+existe", status_code=303)
    db.add(
        Branch(
            code=code,
            name=name,
            company_name=company_name,
            ruc=ruc,
            telefono=telefono,
            direccion=direccion,
        )
    )
    db.commit()
    return RedirectResponse("/data/sucursales?success=Sucursal+creada", status_code=303)


@router.post("/data/sucursales/{item_id}/update")
def data_update_sucursal(
    item_id: int,
    code: str = Form(...),
    name: str = Form(...),
    company_name: str = Form(...),
    ruc: str = Form(...),
    telefono: str = Form(...),
    direccion: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    branch = db.query(Branch).filter(Branch.id == item_id).first()
    if not branch:
        return RedirectResponse("/data/sucursales?error=Sucursal+no+existe", status_code=303)
    code = code.strip().lower()
    name = name.strip()
    company_name = company_name.strip()
    ruc = ruc.strip()
    telefono = telefono.strip()
    direccion = direccion.strip()
    if not code or not name or not company_name or not ruc or not telefono or not direccion:
        return RedirectResponse("/data/sucursales?error=Datos+incompletos", status_code=303)
    exists = (
        db.query(Branch)
        .filter(Branch.id != item_id)
        .filter((func.lower(Branch.code) == code) | (func.lower(Branch.name) == name.lower()))
        .first()
    )
    if exists:
        return RedirectResponse("/data/sucursales?error=Sucursal+ya+existe", status_code=303)
    branch.code = code
    branch.name = name
    branch.company_name = company_name
    branch.ruc = ruc
    branch.telefono = telefono
    branch.direccion = direccion
    db.commit()
    return RedirectResponse("/data/sucursales?success=Sucursal+actualizada", status_code=303)


@router.get("/data/bodegas")
def data_bodegas(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(Bodega).filter(Bodega.id == int(edit_id)).first()
    items = db.query(Bodega).order_by(Bodega.name).all()
    branches = db.query(Branch).order_by(Branch.name).all()
    return request.app.state.templates.TemplateResponse(
        "data_bodegas.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "branches": branches,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/bodegas")
def data_create_bodega(
    code: str = Form(...),
    name: str = Form(...),
    branch_id: int = Form(...),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    code = code.strip().lower()
    name = name.strip()
    if not code or not name:
        return RedirectResponse("/data/bodegas?error=Datos+incompletos", status_code=303)
    branch = db.query(Branch).filter(Branch.id == branch_id).first()
    if not branch:
        return RedirectResponse("/data/bodegas?error=Sucursal+no+valida", status_code=303)
    exists = (
        db.query(Bodega)
        .filter(func.lower(Bodega.code) == code)
        .first()
    )
    if exists:
        return RedirectResponse("/data/bodegas?error=Bodega+ya+existe", status_code=303)
    db.add(Bodega(code=code, name=name, branch_id=branch.id, activo=activo == "on"))
    db.commit()
    return RedirectResponse("/data/bodegas?success=Bodega+creada", status_code=303)


@router.post("/data/bodegas/{item_id}/update")
def data_update_bodega(
    item_id: int,
    code: str = Form(...),
    name: str = Form(...),
    branch_id: int = Form(...),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    bodega = db.query(Bodega).filter(Bodega.id == item_id).first()
    if not bodega:
        return RedirectResponse("/data/bodegas?error=Bodega+no+existe", status_code=303)
    code = code.strip().lower()
    name = name.strip()
    if not code or not name:
        return RedirectResponse("/data/bodegas?error=Datos+incompletos", status_code=303)
    branch = db.query(Branch).filter(Branch.id == branch_id).first()
    if not branch:
        return RedirectResponse("/data/bodegas?error=Sucursal+no+valida", status_code=303)
    exists = (
        db.query(Bodega)
        .filter(Bodega.id != item_id)
        .filter(func.lower(Bodega.code) == code)
        .first()
    )
    if exists:
        return RedirectResponse("/data/bodegas?error=Bodega+ya+existe", status_code=303)
    bodega.code = code
    bodega.name = name
    bodega.branch_id = branch.id
    bodega.activo = activo == "on"
    db.commit()
    return RedirectResponse("/data/bodegas?success=Bodega+actualizada", status_code=303)


@router.get("/data/formas-pago")
def data_formas_pago(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(FormaPago).filter(FormaPago.id == int(edit_id)).first()
    items = db.query(FormaPago).order_by(FormaPago.nombre).all()
    return request.app.state.templates.TemplateResponse(
        "data_formas_pago.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/formas-pago")
def data_create_forma_pago(
    nombre: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        return RedirectResponse("/data/formas-pago?error=Nombre+requerido", status_code=303)
    exists = db.query(FormaPago).filter(FormaPago.nombre == nombre).first()
    if exists:
        return RedirectResponse("/data/formas-pago?error=Forma+ya+existe", status_code=303)
    forma = FormaPago(nombre=nombre)
    db.add(forma)
    db.commit()
    return RedirectResponse("/data/formas-pago?success=Forma+creada", status_code=303)


@router.post("/data/formas-pago/{item_id}/update")
def data_update_forma_pago(
    item_id: int,
    nombre: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    forma = db.query(FormaPago).filter(FormaPago.id == item_id).first()
    if not forma:
        return RedirectResponse("/data/formas-pago?error=Forma+no+existe", status_code=303)
    forma.nombre = nombre.strip()
    db.commit()
    return RedirectResponse("/data/formas-pago?success=Forma+actualizada", status_code=303)


@router.get("/data/recibos-rubros")
def data_recibos_rubros(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(ReciboRubro).filter(ReciboRubro.id == int(edit_id)).first()
    items = db.query(ReciboRubro).order_by(ReciboRubro.nombre).all()
    cuentas = db.query(CuentaContable).filter(CuentaContable.activo.is_(True)).order_by(CuentaContable.codigo).all()
    return request.app.state.templates.TemplateResponse(
        "data_recibos_rubros.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "cuentas": cuentas,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/recibos-rubros")
def data_recibos_rubros_create(
    nombre: str = Form(...),
    cuenta_id: Optional[int] = Form(None),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        return RedirectResponse("/data/recibos-rubros?error=Nombre+requerido", status_code=303)
    exists = db.query(ReciboRubro).filter(func.lower(ReciboRubro.nombre) == nombre.lower()).first()
    if exists:
        return RedirectResponse("/data/recibos-rubros?error=Rubro+ya+existe", status_code=303)
    db.add(ReciboRubro(nombre=nombre, activo=activo == "on", cuenta_id=cuenta_id))
    db.commit()
    return RedirectResponse("/data/recibos-rubros?success=Rubro+creado", status_code=303)


@router.post("/data/recibos-rubros/{item_id}/update")
def data_recibos_rubros_update(
    item_id: int,
    nombre: str = Form(...),
    cuenta_id: Optional[int] = Form(None),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    item = db.query(ReciboRubro).filter(ReciboRubro.id == item_id).first()
    if not item:
        return RedirectResponse("/data/recibos-rubros?error=Rubro+no+existe", status_code=303)
    item.nombre = nombre.strip()
    item.activo = activo == "on"
    item.cuenta_id = cuenta_id
    db.commit()
    return RedirectResponse("/data/recibos-rubros?success=Rubro+actualizado", status_code=303)


@router.get("/data/recibos-motivos")
def data_recibos_motivos(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(ReciboMotivo).filter(ReciboMotivo.id == int(edit_id)).first()
    items = db.query(ReciboMotivo).order_by(ReciboMotivo.tipo, ReciboMotivo.nombre).all()
    return request.app.state.templates.TemplateResponse(
        "data_recibos_motivos.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/recibos-motivos")
def data_recibos_motivos_create(
    nombre: str = Form(...),
    tipo: str = Form(...),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    tipo = tipo.strip().upper()
    if tipo not in {"INGRESO", "EGRESO"}:
        return RedirectResponse("/data/recibos-motivos?error=Tipo+no+valido", status_code=303)
    if not nombre:
        return RedirectResponse("/data/recibos-motivos?error=Nombre+requerido", status_code=303)
    exists = db.query(ReciboMotivo).filter(func.lower(ReciboMotivo.nombre) == nombre.lower()).first()
    if exists:
        return RedirectResponse("/data/recibos-motivos?error=Motivo+ya+existe", status_code=303)
    db.add(ReciboMotivo(nombre=nombre, tipo=tipo, activo=activo == "on"))
    db.commit()
    return RedirectResponse("/data/recibos-motivos?success=Motivo+creado", status_code=303)


@router.post("/data/recibos-motivos/{item_id}/update")
def data_recibos_motivos_update(
    item_id: int,
    nombre: str = Form(...),
    tipo: str = Form(...),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    item = db.query(ReciboMotivo).filter(ReciboMotivo.id == item_id).first()
    if not item:
        return RedirectResponse("/data/recibos-motivos?error=Motivo+no+existe", status_code=303)
    item.nombre = nombre.strip()
    item.tipo = tipo.strip().upper()
    item.activo = activo == "on"
    db.commit()
    return RedirectResponse("/data/recibos-motivos?success=Motivo+actualizado", status_code=303)


@router.get("/data/cuentas-contables")
def data_cuentas_contables(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(CuentaContable).filter(CuentaContable.id == int(edit_id)).first()
    items = db.query(CuentaContable).order_by(CuentaContable.codigo).all()
    cuentas = db.query(CuentaContable).order_by(CuentaContable.codigo).all()
    return request.app.state.templates.TemplateResponse(
        "data_cuentas_contables.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "cuentas": cuentas,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/cuentas-contables")
def data_cuentas_contables_create(
    codigo: str = Form(...),
    nombre: str = Form(...),
    tipo: str = Form(...),
    naturaleza: str = Form(...),
    parent_id: Optional[int] = Form(None),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    codigo = codigo.strip()
    nombre = nombre.strip()
    tipo = tipo.strip().upper()
    naturaleza = naturaleza.strip().upper()
    if not codigo or not nombre:
        return RedirectResponse("/data/cuentas-contables?error=Datos+incompletos", status_code=303)
    if tipo not in {"BALANCE", "RESULTADO"}:
        return RedirectResponse("/data/cuentas-contables?error=Tipo+no+valido", status_code=303)
    if naturaleza not in {"DEBE", "HABER"}:
        return RedirectResponse("/data/cuentas-contables?error=Naturaleza+no+valida", status_code=303)
    exists = db.query(CuentaContable).filter(func.lower(CuentaContable.codigo) == codigo.lower()).first()
    if exists:
        return RedirectResponse("/data/cuentas-contables?error=Codigo+ya+existe", status_code=303)
    nivel = 1
    if parent_id:
        parent = db.query(CuentaContable).filter(CuentaContable.id == parent_id).first()
        if not parent:
            return RedirectResponse("/data/cuentas-contables?error=Cuenta+padre+no+valida", status_code=303)
        nivel = (parent.nivel or 1) + 1
        if nivel > 4:
            return RedirectResponse("/data/cuentas-contables?error=Maximo+4+niveles", status_code=303)
    db.add(
        CuentaContable(
            codigo=codigo,
            nombre=nombre,
            tipo=tipo,
            naturaleza=naturaleza,
            parent_id=parent_id,
            nivel=nivel,
            activo=activo == "on",
        )
    )
    db.commit()
    return RedirectResponse("/data/cuentas-contables?success=Cuenta+creada", status_code=303)


@router.post("/data/cuentas-contables/{item_id}/update")
def data_cuentas_contables_update(
    item_id: int,
    codigo: str = Form(...),
    nombre: str = Form(...),
    tipo: str = Form(...),
    naturaleza: str = Form(...),
    parent_id: Optional[int] = Form(None),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    item = db.query(CuentaContable).filter(CuentaContable.id == item_id).first()
    if not item:
        return RedirectResponse("/data/cuentas-contables?error=Cuenta+no+existe", status_code=303)
    codigo = codigo.strip()
    nombre = nombre.strip()
    tipo = tipo.strip().upper()
    naturaleza = naturaleza.strip().upper()
    exists = (
        db.query(CuentaContable)
        .filter(CuentaContable.id != item_id)
        .filter(func.lower(CuentaContable.codigo) == codigo.lower())
        .first()
    )
    if exists:
        return RedirectResponse("/data/cuentas-contables?error=Codigo+ya+existe", status_code=303)
    nivel = 1
    if parent_id:
        parent = db.query(CuentaContable).filter(CuentaContable.id == parent_id).first()
        if not parent:
            return RedirectResponse("/data/cuentas-contables?error=Cuenta+padre+no+valida", status_code=303)
        nivel = (parent.nivel or 1) + 1
        if nivel > 4:
            return RedirectResponse("/data/cuentas-contables?error=Maximo+4+niveles", status_code=303)
    item.codigo = codigo
    item.nombre = nombre
    item.tipo = tipo
    item.naturaleza = naturaleza
    item.parent_id = parent_id
    item.nivel = nivel
    item.activo = activo == "on"
    db.commit()
    return RedirectResponse("/data/cuentas-contables?success=Cuenta+actualizada", status_code=303)


@router.get("/data/roles")
def data_roles(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.roles")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(Role).filter(Role.id == int(edit_id)).first()
    items = db.query(Role).order_by(Role.name).all()
    return request.app.state.templates.TemplateResponse(
        "data_roles.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/roles")
def data_create_role(
    nombre: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    nombre = nombre.strip().lower()
    if not nombre:
        return RedirectResponse("/data/roles?error=Nombre+requerido", status_code=303)
    exists = db.query(Role).filter(Role.name == nombre).first()
    if exists:
        return RedirectResponse("/data/roles?error=Rol+ya+existe", status_code=303)
    role = Role(name=nombre)
    db.add(role)
    db.commit()
    return RedirectResponse("/data/roles?success=Rol+creado", status_code=303)


@router.post("/data/roles/{item_id}/update")
def data_update_role(
    item_id: int,
    nombre: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    role = db.query(Role).filter(Role.id == item_id).first()
    if not role:
        return RedirectResponse("/data/roles?error=Rol+no+existe", status_code=303)
    role.name = nombre.strip().lower()
    db.commit()
    return RedirectResponse("/data/roles?success=Rol+actualizado", status_code=303)


@router.get("/data/permisos")
def data_permisos(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.permissions")
    role_id = request.query_params.get("role_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    roles = db.query(Role).order_by(Role.name).all()
    selected_role = None
    if role_id:
        selected_role = db.query(Role).filter(Role.id == int(role_id)).first()
    if not selected_role and roles:
        selected_role = roles[0]
    selected_permissions = set()
    if selected_role:
        if selected_role.name == "administrador":
            selected_permissions = set(_permission_catalog_names())
        else:
            selected_permissions = {perm.name for perm in (selected_role.permissions or [])}
    return request.app.state.templates.TemplateResponse(
        "data_permisos.html",
        {
            "request": request,
            "user": user,
            "roles": roles,
            "selected_role": selected_role,
            "selected_permissions": selected_permissions,
            "permission_groups": PERMISSION_GROUPS,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/permisos")
def data_permisos_update(
    request: Request,
    role_id: int = Form(...),
    perms: Optional[list[str]] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.permissions")
    role = db.query(Role).filter(Role.id == role_id).first()
    if not role:
        return RedirectResponse("/data/permisos?error=Rol+no+existe", status_code=303)
    perm_names = _permission_catalog_names()
    selected = set(perms or [])
    selected = {name for name in selected if name in perm_names}
    permissions = []
    if role.name == "administrador":
        permissions = db.query(Permission).filter(Permission.name.in_(perm_names)).all()
    else:
        if selected:
            permissions = db.query(Permission).filter(Permission.name.in_(selected)).all()
    role.permissions = permissions
    db.commit()
    return RedirectResponse(
        f"/data/permisos?role_id={role_id}&success=Permisos+actualizados",
        status_code=303,
    )


@router.get("/data/usuarios")
def data_usuarios(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.users")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(User).filter(User.id == int(edit_id)).first()
    items = db.query(User).order_by(User.full_name).all()
    roles = db.query(Role).order_by(Role.name).all()
    branches = db.query(Branch).order_by(Branch.name).all()
    bodegas = db.query(Bodega).order_by(Bodega.name).all()
    return request.app.state.templates.TemplateResponse(
        "data_usuarios.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "roles": roles,
            "branches": branches,
            "bodegas": bodegas,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/usuarios")
def data_create_usuario(
    full_name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    role_ids: Optional[list[int]] = Form(None),
    branch_id: Optional[int] = Form(None),
    bodega_id: Optional[int] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    full_name = full_name.strip()
    email = email.strip().lower()
    if not full_name or not email or not password:
        return RedirectResponse("/data/usuarios?error=Datos+incompletos", status_code=303)
    exists = db.query(User).filter(func.lower(User.email) == email).first()
    if exists:
        return RedirectResponse("/data/usuarios?error=Email+ya+existe", status_code=303)
    roles = []
    if role_ids:
        roles = db.query(Role).filter(Role.id.in_(role_ids)).all()
    if not branch_id or not bodega_id:
        return RedirectResponse("/data/usuarios?error=Sucursal+y+bodega+requeridas", status_code=303)
    branch = db.query(Branch).filter(Branch.id == branch_id).first()
    if not branch:
        return RedirectResponse("/data/usuarios?error=Sucursal+no+valida", status_code=303)
    bodega = db.query(Bodega).filter(Bodega.id == bodega_id).first()
    if not bodega or bodega.branch_id != branch.id:
        return RedirectResponse("/data/usuarios?error=Bodega+no+corresponde+a+la+sucursal", status_code=303)
    new_user = User(
        full_name=full_name,
        email=email,
        hashed_password=hash_password(password),
        is_active=True,
        roles=roles,
        branches=[branch],
        default_branch_id=branch.id,
        default_bodega_id=bodega.id,
    )
    db.add(new_user)
    db.commit()
    return RedirectResponse("/data/usuarios?success=Usuario+creado", status_code=303)


@router.post("/data/usuarios/{item_id}/update")
def data_update_usuario(
    item_id: int,
    full_name: str = Form(...),
    email: str = Form(...),
    new_password: Optional[str] = Form(None),
    role_ids: Optional[list[int]] = Form(None),
    is_active: Optional[str] = Form(None),
    branch_id: Optional[int] = Form(None),
    bodega_id: Optional[int] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    edit_user = db.query(User).filter(User.id == item_id).first()
    if not edit_user:
        return RedirectResponse("/data/usuarios?error=Usuario+no+existe", status_code=303)
    edit_user.full_name = full_name.strip()
    edit_user.email = email.strip().lower()
    if new_password:
        edit_user.hashed_password = hash_password(new_password)
    edit_user.is_active = is_active == "on"
    roles = []
    if role_ids:
        roles = db.query(Role).filter(Role.id.in_(role_ids)).all()
    edit_user.roles = roles
    if not branch_id or not bodega_id:
        return RedirectResponse("/data/usuarios?error=Sucursal+y+bodega+requeridas", status_code=303)
    branch = db.query(Branch).filter(Branch.id == branch_id).first()
    if not branch:
        return RedirectResponse("/data/usuarios?error=Sucursal+no+valida", status_code=303)
    bodega = db.query(Bodega).filter(Bodega.id == bodega_id).first()
    if not bodega or bodega.branch_id != branch.id:
        return RedirectResponse("/data/usuarios?error=Bodega+no+corresponde+a+la+sucursal", status_code=303)
    edit_user.branches = [branch]
    edit_user.default_branch_id = branch.id
    edit_user.default_bodega_id = bodega.id
    db.commit()
    return RedirectResponse("/data/usuarios?success=Usuario+actualizado", status_code=303)


@router.get("/data/clientes")
def data_clientes(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    _enforce_permission(request, user, "access.data.catalogs")
    edit_id = request.query_params.get("edit_id")
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    edit_item = None
    if edit_id:
        edit_item = db.query(Cliente).filter(Cliente.id == int(edit_id)).first()
    items = db.query(Cliente).order_by(Cliente.nombre).all()
    return request.app.state.templates.TemplateResponse(
        "data_clientes.html",
        {
            "request": request,
            "user": user,
            "items": items,
            "edit_item": edit_item,
            "error": error,
            "success": success,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/data/clientes")
def data_create_cliente(
    nombre: str = Form(...),
    identificacion: Optional[str] = Form(None),
    telefono: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    direccion: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        return RedirectResponse("/data/clientes?error=Nombre+requerido", status_code=303)
    exists = db.query(Cliente).filter(func.lower(Cliente.nombre) == nombre.lower()).first()
    if exists:
        return RedirectResponse("/data/clientes?error=Cliente+ya+existe", status_code=303)
    cliente = Cliente(
        nombre=nombre,
        identificacion=identificacion.strip() if identificacion else None,
        telefono=telefono,
        email=email,
        direccion=direccion,
        activo=True,
    )
    db.add(cliente)
    db.commit()
    return RedirectResponse("/data/clientes?success=Cliente+creado", status_code=303)


@router.post("/data/clientes/{item_id}/update")
def data_update_cliente(
    item_id: int,
    nombre: str = Form(...),
    identificacion: Optional[str] = Form(None),
    telefono: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    direccion: Optional[str] = Form(None),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    cliente = db.query(Cliente).filter(Cliente.id == item_id).first()
    if not cliente:
        return RedirectResponse("/data/clientes?error=Cliente+no+existe", status_code=303)
    cliente.nombre = nombre.strip()
    cliente.identificacion = identificacion.strip() if identificacion else None
    cliente.telefono = telefono
    cliente.email = email
    cliente.direccion = direccion
    cliente.activo = activo == "on"
    db.commit()
    return RedirectResponse("/data/clientes?success=Cliente+actualizado", status_code=303)

@router.get("/sales/products/search")
def sales_products_search(
    q: str = "",
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    query = q.strip()
    if len(query) < 2:
        return JSONResponse({"ok": True, "items": []})
    like = f"%{query.lower()}%"
    productos = (
        db.query(Producto)
        .filter(Producto.activo.is_(True))
        .filter(
            or_(
                func.lower(Producto.cod_producto).like(like),
                func.lower(Producto.descripcion).like(like),
            )
        )
        .order_by(Producto.descripcion)
        .limit(100)
        .all()
    )
    _, bodega = _resolve_branch_bodega(db, user)
    balances: dict[tuple[int, int], Decimal] = {}
    if bodega and productos:
        product_ids = [p.id for p in productos]
        balances = _balances_by_bodega(db, [bodega.id], product_ids)
    items = []
    for producto in productos:
        existencia = 0.0
        if bodega and balances:
            existencia = float(balances.get((producto.id, bodega.id), Decimal("0")) or 0)
        elif producto.saldo:
            existencia = float(producto.saldo.existencia or 0)
        items.append(
            {
                "id": producto.id,
                "cod_producto": producto.cod_producto,
                "descripcion": producto.descripcion,
                "precio_venta1_usd": float(producto.precio_venta1_usd or 0),
                "precio_venta1": float(producto.precio_venta1 or 0),
                "existencia": existencia,
                "combo_count": len(producto.combo_children or []),
            }
        )
    return JSONResponse({"ok": True, "items": items})


@router.get("/inventory/ingresos/{ingreso_id}/pdf")
def inventory_ingreso_pdf(
    ingreso_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="ReportLab no esta instalado") from exc

    ingreso = (
        db.query(IngresoInventario)
        .filter(IngresoInventario.id == ingreso_id)
        .first()
    )
    if not ingreso:
        raise HTTPException(status_code=404, detail="Ingreso no encontrado")

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    margin = 36

    logo_path = Path(__file__).resolve().parents[1] / "static" / "logo_hollywood.png"
    if logo_path.exists():
        pdf.drawImage(
            str(logo_path),
            margin,
            height - 78,
            width=90,
            height=60,
            preserveAspectRatio=True,
            mask="auto",
        )

    info_x = margin + 110
    info_y = height - 44
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(info_x, info_y, "Pacas Hollywood Managua")
    pdf.setFont("Helvetica", 9)
    pdf.drawString(info_x, info_y - 14, "Telf. 8900-0300")
    pdf.drawString(
        info_x,
        info_y - 28,
        "Direccion: Semaforos del colonial 20 vrs. abajo frente al pillin.",
    )

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(margin, height - 120, "Informe de ingreso de mercaderias")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(margin, height - 136, f"Reimpresion ingreso #{ingreso.id}")
    pdf.setStrokeColorRGB(0.75, 0.75, 0.75)
    pdf.setLineWidth(0.6)
    pdf.line(margin, height - 146, width - margin, height - 146)

    pdf.setFont("Helvetica", 9)
    pdf.drawString(
        margin,
        height - 160,
        f"Fecha: {ingreso.fecha.isoformat()}",
    )
    pdf.drawString(
        margin + 200,
        height - 160,
        f"Tipo: {ingreso.tipo.nombre if ingreso.tipo else '-'}",
    )
    pdf.drawString(
        margin,
        height - 174,
        f"Bodega: {ingreso.bodega.name if ingreso.bodega else '-'}",
    )
    pdf.drawString(
        margin + 200,
        height - 174,
        f"Proveedor: {ingreso.proveedor.nombre if ingreso.proveedor else '-'}",
    )
    pdf.drawString(
        margin,
        height - 188,
        f"Moneda: {ingreso.moneda}",
    )
    pdf.drawString(
        margin + 200,
        height - 188,
        f"Tasa: {('C$ %.4f' % float(ingreso.tasa_cambio)) if ingreso.tasa_cambio else '-'}",
    )
    observacion = ingreso.observacion or "-"
    if len(observacion) > 120:
        observacion = f"{observacion[:117]}..."
    pdf.drawString(
        margin,
        height - 202,
        f"Descripcion: {observacion}",
    )

    y = height - 224
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(margin, y, "Codigo")
    pdf.drawString(margin + 80, y, "Descripcion")
    pdf.drawRightString(margin + 340, y, "Cant.")
    pdf.drawRightString(margin + 420, y, "Costo USD")
    pdf.drawRightString(margin + 500, y, "Subtotal USD")
    y -= 12

    pdf.setFont("Helvetica", 8)
    for item in ingreso.items:
        if y < margin + 60:
            pdf.setFont("Helvetica", 8)
            pdf.drawRightString(
                width - margin,
                margin - 18,
                f"Pagina {pdf.getPageNumber()}",
            )
            pdf.showPage()
            y = height - margin
            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(margin, y, "Codigo")
            pdf.drawString(margin + 80, y, "Descripcion")
            pdf.drawRightString(margin + 340, y, "Cant.")
            pdf.drawRightString(margin + 420, y, "Costo USD")
            pdf.drawRightString(margin + 500, y, "Subtotal USD")
            y -= 12
            pdf.setFont("Helvetica", 8)

        codigo = item.producto.cod_producto if item.producto else ""
        descripcion = item.producto.descripcion if item.producto else ""
        if len(descripcion) > 48:
            descripcion = f"{descripcion[:45]}..."
        pdf.drawString(margin, y, codigo)
        pdf.drawString(margin + 80, y, descripcion)
        pdf.drawRightString(margin + 340, y, f"{float(item.cantidad or 0):.2f}")
        pdf.drawRightString(margin + 420, y, f"{float(item.costo_unitario_usd or 0):.2f}")
        pdf.drawRightString(margin + 500, y, f"{float(item.subtotal_usd or 0):.2f}")
        y -= 12

    y -= 8
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawRightString(margin + 420, y, "Total USD:")
    pdf.drawRightString(margin + 500, y, f"{float(ingreso.total_usd or 0):.2f}")
    y -= 12
    pdf.drawRightString(margin + 420, y, "Total C$:")
    pdf.drawRightString(margin + 500, y, f"{float(ingreso.total_cs or 0):.2f}")

    pdf.setFont("Helvetica", 8)
    pdf.drawRightString(
        width - margin,
        margin - 18,
        f"Pagina {pdf.getPageNumber()}",
    )

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    headers = {
        "Content-Disposition": f"inline; filename=ingreso_{ingreso.id}.pdf"
    }
    return StreamingResponse(buffer, media_type="application/pdf", headers=headers)


@router.get("/inventory/egresos/{egreso_id}/pdf")
def inventory_egreso_pdf(
    egreso_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="ReportLab no esta instalado") from exc

    egreso = (
        db.query(EgresoInventario)
        .filter(EgresoInventario.id == egreso_id)
        .first()
    )
    if not egreso:
        raise HTTPException(status_code=404, detail="Egreso no encontrado")

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    margin = 36

    logo_path = Path(__file__).resolve().parents[1] / "static" / "logo_hollywood.png"
    if logo_path.exists():
        pdf.drawImage(
            str(logo_path),
            margin,
            height - 78,
            width=90,
            height=60,
            preserveAspectRatio=True,
            mask="auto",
        )

    info_x = margin + 110
    info_y = height - 44
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(info_x, info_y, "Pacas Hollywood Managua")
    pdf.setFont("Helvetica", 9)
    pdf.drawString(info_x, info_y - 14, "Telf. 8900-0300")
    pdf.drawString(
        info_x,
        info_y - 28,
        "Direccion: Semaforos del colonial 20 vrs. abajo frente al pillin.",
    )

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(margin, height - 120, "Informe de egreso de mercaderias")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(margin, height - 136, f"Reimpresion egreso #{egreso.id}")
    pdf.setStrokeColorRGB(0.75, 0.75, 0.75)
    pdf.setLineWidth(0.6)
    pdf.line(margin, height - 146, width - margin, height - 146)

    pdf.setFont("Helvetica", 9)
    pdf.drawString(
        margin,
        height - 160,
        f"Fecha: {egreso.fecha.isoformat()}",
    )
    pdf.drawString(
        margin + 200,
        height - 160,
        f"Tipo: {egreso.tipo.nombre if egreso.tipo else '-'}",
    )
    pdf.drawString(
        margin,
        height - 174,
        f"Bodega: {egreso.bodega.name if egreso.bodega else '-'}",
    )
    pdf.drawString(
        margin,
        height - 188,
        f"Moneda: {egreso.moneda}",
    )
    pdf.drawString(
        margin + 200,
        height - 188,
        f"Tasa: {('C$ %.4f' % float(egreso.tasa_cambio)) if egreso.tasa_cambio else '-'}",
    )
    observacion = egreso.observacion or "-"
    if len(observacion) > 120:
        observacion = f"{observacion[:117]}..."
    pdf.drawString(
        margin,
        height - 202,
        f"Motivo: {observacion}",
    )

    y = height - 224
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(margin, y, "Codigo")
    pdf.drawString(margin + 80, y, "Descripcion")
    pdf.drawRightString(margin + 340, y, "Cant.")
    pdf.drawRightString(margin + 420, y, "Costo USD")
    pdf.drawRightString(margin + 500, y, "Subtotal USD")
    y -= 12

    pdf.setFont("Helvetica", 8)
    for item in egreso.items:
        if y < margin + 60:
            pdf.setFont("Helvetica", 8)
            pdf.drawRightString(
                width - margin,
                margin - 18,
                f"Pagina {pdf.getPageNumber()}",
            )
            pdf.showPage()
            y = height - margin
            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(margin, y, "Codigo")
            pdf.drawString(margin + 80, y, "Descripcion")
            pdf.drawRightString(margin + 340, y, "Cant.")
            pdf.drawRightString(margin + 420, y, "Costo USD")
            pdf.drawRightString(margin + 500, y, "Subtotal USD")
            y -= 12
            pdf.setFont("Helvetica", 8)

        codigo = item.producto.cod_producto if item.producto else ""
        descripcion = item.producto.descripcion if item.producto else ""
        if len(descripcion) > 48:
            descripcion = f"{descripcion[:45]}..."
        pdf.drawString(margin, y, codigo)
        pdf.drawString(margin + 80, y, descripcion)
        pdf.drawRightString(margin + 340, y, f"{float(item.cantidad or 0):.2f}")
        pdf.drawRightString(margin + 420, y, f"{float(item.costo_unitario_usd or 0):.2f}")
        pdf.drawRightString(margin + 500, y, f"{float(item.subtotal_usd or 0):.2f}")
        y -= 12

    y -= 8
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawRightString(margin + 420, y, "Total USD:")
    pdf.drawRightString(margin + 500, y, f"{float(egreso.total_usd or 0):.2f}")
    y -= 12
    pdf.drawRightString(margin + 420, y, "Total C$:")
    pdf.drawRightString(margin + 500, y, f"{float(egreso.total_cs or 0):.2f}")

    pdf.setFont("Helvetica", 8)
    pdf.drawRightString(
        width - margin,
        margin - 18,
        f"Pagina {pdf.getPageNumber()}",
    )

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    headers = {
        "Content-Disposition": f"inline; filename=egreso_{egreso.id}.pdf"
    }
    return StreamingResponse(buffer, media_type="application/pdf", headers=headers)


@router.get("/sales/{venta_id}/pdf")
def sales_invoice_pdf(
    venta_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="ReportLab no esta instalado") from exc

    factura = (
        db.query(VentaFactura)
        .filter(VentaFactura.id == venta_id)
        .first()
    )
    if not factura:
        raise HTTPException(status_code=404, detail="Factura no encontrada")

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    margin = 36

    logo_path = Path(__file__).resolve().parents[1] / "static" / "logo_hollywood.png"
    if logo_path.exists():
        pdf.drawImage(
            str(logo_path),
            margin,
            height - 78,
            width=90,
            height=60,
            preserveAspectRatio=True,
            mask="auto",
        )

    info_x = margin + 110
    info_y = height - 44
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(info_x, info_y, "Pacas Hollywood Managua")
    pdf.setFont("Helvetica", 9)
    pdf.drawString(info_x, info_y - 14, "Telf. 8900-0300")
    pdf.drawString(
        info_x,
        info_y - 28,
        "Direccion: Semaforos del colonial 20 vrs. abajo frente al pillin.",
    )

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(margin, height - 120, "Factura de venta")
    pdf.setFont("Helvetica", 11)
    pdf.drawString(margin, height - 136, f"{factura.numero}")
    pdf.setStrokeColorRGB(0.75, 0.75, 0.75)
    pdf.setLineWidth(0.6)
    pdf.line(margin, height - 146, width - margin, height - 146)

    pdf.setFont("Helvetica", 9)
    pdf.drawString(
        margin,
        height - 160,
        f"Fecha: {factura.fecha.date().isoformat()}",
    )
    pdf.drawString(
        margin + 200,
        height - 160,
        f"Vendedor: {factura.vendedor.nombre if factura.vendedor else '-'}",
    )
    pdf.drawString(
        margin,
        height - 174,
        f"Cliente: {factura.cliente.nombre if factura.cliente else 'Consumidor final'}",
    )
    pdf.drawString(
        margin + 200,
        height - 174,
        f"Moneda: {factura.moneda}",
    )
    pdf.drawString(
        margin,
        height - 188,
        f"Bodega: {factura.bodega.name if factura.bodega else '-'}",
    )
    pdf.drawString(
        margin + 200,
        height - 188,
        f"Tasa: {('C$ %.4f' % float(factura.tasa_cambio)) if factura.tasa_cambio else '-'}",
    )

    y = height - 210
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(margin, y, "Codigo")
    pdf.drawString(margin + 80, y, "Descripcion")
    pdf.drawRightString(margin + 340, y, "Cant.")
    pdf.drawRightString(margin + 420, y, "Precio USD")
    pdf.drawRightString(margin + 500, y, "Subtotal USD")
    y -= 12

    pdf.setFont("Helvetica", 8)
    for item in factura.items:
        if y < margin + 60:
            pdf.setFont("Helvetica", 8)
            pdf.drawRightString(
                width - margin,
                margin - 18,
                f"Pagina {pdf.getPageNumber()}",
            )
            pdf.showPage()
            y = height - margin
            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(margin, y, "Codigo")
            pdf.drawString(margin + 80, y, "Descripcion")
            pdf.drawRightString(margin + 340, y, "Cant.")
            pdf.drawRightString(margin + 420, y, "Precio USD")
            pdf.drawRightString(margin + 500, y, "Subtotal USD")
            y -= 12
            pdf.setFont("Helvetica", 8)

        codigo = item.producto.cod_producto if item.producto else ""
        descripcion = item.producto.descripcion if item.producto else ""
        if len(descripcion) > 48:
            descripcion = f"{descripcion[:45]}..."
        pdf.drawString(margin, y, codigo)
        pdf.drawString(margin + 80, y, descripcion)
        pdf.drawRightString(margin + 340, y, f"{float(item.cantidad or 0):.2f}")
        pdf.drawRightString(margin + 420, y, f"{float(item.precio_unitario_usd or 0):.2f}")
        pdf.drawRightString(margin + 500, y, f"{float(item.subtotal_usd or 0):.2f}")
        y -= 12

    y -= 8
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawRightString(margin + 420, y, "Total USD:")
    pdf.drawRightString(margin + 500, y, f"{float(factura.total_usd or 0):.2f}")
    y -= 12
    pdf.drawRightString(margin + 420, y, "Total C$:")
    pdf.drawRightString(margin + 500, y, f"{float(factura.total_cs or 0):.2f}")

    pago = factura.pagos[0] if factura.pagos else None
    if pago:
        y -= 18
        pdf.setFont("Helvetica", 9)
        pdf.drawString(
            margin,
            y,
            f"Forma de pago: {pago.forma_pago.nombre if pago.forma_pago else '-'}",
        )
        banco = pago.banco.nombre if pago.banco else "-"
        pdf.drawString(margin + 200, y, f"Banco: {banco}")

    pdf.setFont("Helvetica", 8)
    pdf.drawRightString(
        width - margin,
        margin - 18,
        f"Pagina {pdf.getPageNumber()}",
    )

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    headers = {"Content-Disposition": f"inline; filename={factura.numero}.pdf"}
    return StreamingResponse(buffer, media_type="application/pdf", headers=headers)


@router.get("/sales/{venta_id}/ticket")
def sales_ticket_pos(
    venta_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    try:
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas
    except ImportError as exc:
        raise HTTPException(status_code=500, detail="ReportLab no esta instalado") from exc

    factura = (
        db.query(VentaFactura)
        .filter(VentaFactura.id == venta_id)
        .first()
    )
    if not factura:
        raise HTTPException(status_code=404, detail="Factura no encontrada")

    pdf_bytes = _build_pos_ticket_pdf_bytes(factura)
    buffer = io.BytesIO(pdf_bytes)
    headers = {"Content-Disposition": f"inline; filename={factura.numero}_ticket.pdf"}
    return StreamingResponse(buffer, media_type="application/pdf", headers=headers)


@router.get("/sales/{venta_id}/ticket/print")
def sales_ticket_print(
    request: Request,
    venta_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    copies_value = request.query_params.get("copies", "1")
    try:
        copies = max(int(copies_value), 1)
    except ValueError:
        copies = 1
    factura = (
        db.query(VentaFactura)
        .filter(VentaFactura.id == venta_id)
        .first()
    )
    if not factura:
        raise HTTPException(status_code=404, detail="Factura no encontrada")

    def wrap_text(text: str, max_chars: int) -> list[str]:
        if not text:
            return [""]
        words = text.split()
        lines: list[str] = []
        current = ""
        for word in words:
            candidate = f"{current} {word}".strip()
            if len(candidate) > max_chars:
                if current:
                    lines.append(current)
                current = word
            else:
                current = candidate
        if current:
            lines.append(current)
        return lines or [text]

    branch = factura.bodega.branch if factura.bodega else None
    company_name = (branch.company_name if branch and branch.company_name else "Pacas Hollywood").strip()
    ruc = branch.ruc if branch and branch.ruc else "-"
    telefono = branch.telefono if branch and branch.telefono else "-"
    direccion = branch.direccion if branch and branch.direccion else "-"
    direccion_lines = wrap_text(direccion, 32)[:2]
    sucursal = branch.name if branch and branch.name else "-"

    cliente = factura.cliente.nombre if factura.cliente else "Consumidor final"
    cliente_id = factura.cliente.identificacion if factura.cliente and factura.cliente.identificacion else "-"
    vendedor = factura.vendedor.nombre if factura.vendedor else "-"

    fecha_base = factura.created_at or factura.fecha
    fecha_str = ""
    hora_str = ""
    if fecha_base:
        try:
            fecha_str = fecha_base.strftime("%d/%m/%Y")
            hora_str = fecha_base.strftime("%H:%M")
        except AttributeError:
            fecha_str = str(fecha_base)

    moneda = factura.moneda or "CS"
    currency_label = "C$" if moneda == "CS" else "$"
    total_amount = float(factura.total_cs or 0) if moneda == "CS" else float(factura.total_usd or 0)
    subtotal_amount = total_amount

    pagos = factura.pagos or []
    total_paid = sum(
        float(pago.monto_cs or 0) if moneda == "CS" else float(pago.monto_usd or 0)
        for pago in pagos
    )
    saldo = total_paid - total_amount

    def format_amount(value: float) -> str:
        return f"{value:,.2f}"

    items = []
    line_count = 0
    line_count += 1  # company
    line_count += 1  # ruc
    line_count += 1  # telefono
    line_count += len(direccion_lines)
    line_count += 1  # sucursal
    line_count += 1  # divider
    line_count += 5  # factura/fecha/cliente/id/vendedor
    line_count += 1  # divider
    for item in factura.items:
        desc_lines = wrap_text(item.producto.descripcion if item.producto else "-", 32)
        line_count += 1  # codigo
        line_count += len(desc_lines)
        line_count += 2  # qty/price + desc/subtotal
        line_count += 1  # divider
        qty = float(item.cantidad or 0)
        price = (
            float(item.precio_unitario_cs or 0)
            if moneda == "CS"
            else float(item.precio_unitario_usd or 0)
        )
        subtotal = (
            float(item.subtotal_cs or 0)
            if moneda == "CS"
            else float(item.subtotal_usd or 0)
        )
        items.append(
            {
                "codigo": item.producto.cod_producto if item.producto else "-",
                "descripcion": item.producto.descripcion if item.producto else "-",
                "cantidad": qty,
                "precio": price,
                "subtotal": subtotal,
            }
        )

    pagos_render = []
    line_count += 3  # subtotal/desc/total
    for pago in pagos:
        forma = pago.forma_pago.nombre if pago.forma_pago else "Pago"
        banco = pago.banco.nombre if pago.banco else ""
        label = f"{forma} {banco}".strip()
        monto = (
            float(pago.monto_cs or 0)
            if moneda == "CS"
            else float(pago.monto_usd or 0)
        )
        pagos_render.append({"label": label, "monto": monto})
    if pagos_render:
        line_count += 2  # divider + title
        line_count += len(pagos_render)
    line_count += 1  # vuelto/saldo
    line_count += 4  # footer

    line_height_mm = 4.0
    page_height_mm = max(120.0, 14.0 + line_count * line_height_mm + 18.0)

    return request.app.state.templates.TemplateResponse(
        "sales_ticket_print.html",
        {
            "request": request,
            "factura": factura,
            "company_name": company_name,
            "ruc": ruc,
            "telefono": telefono,
            "direccion": direccion,
            "direccion_lines": direccion_lines,
            "sucursal": sucursal,
            "cliente": cliente,
            "cliente_id": cliente_id,
            "vendedor": vendedor,
            "fecha_str": fecha_str,
            "hora_str": hora_str,
            "moneda": moneda,
            "currency_label": currency_label,
            "total_amount": total_amount,
            "subtotal_amount": subtotal_amount,
            "saldo": saldo,
            "items": items,
            "pagos": pagos_render,
            "format_amount": format_amount,
            "copies": copies,
            "page_height_mm": page_height_mm,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/inventory/product")
def inventory_create_product(
    request: Request,
    cod_producto: str = Form(...),
    descripcion: str = Form(...),
    linea_id: Optional[str] = Form(None),
    segmento_id: Optional[str] = Form(None),
    marca: Optional[str] = Form(None),
    referencia_producto: Optional[str] = Form(None),
    precio_venta1_usd: float = Form(0),
    precio_venta2_usd: float = Form(0),
    precio_venta3_usd: float = Form(0),
    costo_producto_usd: float = Form(0),
    existencia: float = Form(0),
    activo: Optional[str] = Form(None),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    accept_header = request.headers.get("accept", "")
    is_fetch = (
        request.headers.get("x-requested-with") == "fetch"
        or "application/json" in accept_header
        or request.headers.get("hx-request") == "true"
    )

    def _error(message: str):
        if is_fetch:
            return JSONResponse({"ok": False, "message": message}, status_code=400)
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error={message.replace(' ', '+')}", status_code=303)

    cod_producto = cod_producto.strip()
    descripcion = descripcion.strip()
    if not cod_producto or not descripcion:
        return _error("Faltan datos obligatorios")

    def _to_int(value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        return int(value) if value.isdigit() else None

    exists = db.query(Producto).filter(Producto.cod_producto == cod_producto).first()
    if exists:
        return _error("Codigo existente")

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if not rate_today:
        return _error("Tasa de cambio no configurada")

    tasa = float(rate_today.rate)
    active_flag = True if activo is None else activo == "on"
    producto = Producto(
        cod_producto=cod_producto,
        descripcion=descripcion,
        linea_id=_to_int(linea_id),
        segmento_id=_to_int(segmento_id),
        marca=marca,
        referencia_producto=referencia_producto,
        precio_venta1=precio_venta1_usd * tasa,
        precio_venta2=precio_venta2_usd * tasa,
        precio_venta3=precio_venta3_usd * tasa,
        precio_venta1_usd=precio_venta1_usd,
        precio_venta2_usd=precio_venta2_usd,
        precio_venta3_usd=precio_venta3_usd,
        tasa_cambio=tasa,
        costo_producto=costo_producto_usd * tasa,
        activo=active_flag,
)
    db.add(producto)
    db.flush()
    db.add(SaldoProducto(producto_id=producto.id, existencia=existencia))
    db.commit()
    if is_fetch:
        return JSONResponse(
            {
                "ok": True,
                "message": "Producto registrado exitosamente",
                "id": producto.id,
                "cod_producto": producto.cod_producto,
                "descripcion": producto.descripcion,
                "precio_venta1_usd": float(producto.precio_venta1_usd or 0),
                "costo_usd": float(costo_producto_usd or 0),
                "precio_venta1": float(producto.precio_venta1 or 0),
                "costo_cs": float(producto.costo_producto or 0),
                "activo": bool(producto.activo),
            }
        )
    target = redirect_to or "/inventory"
    return RedirectResponse(f"{target}?success=Producto+registrado", status_code=303)


@router.post("/inventory/product/{product_id}/update")
def inventory_update_product(
    request: Request,
    product_id: int,
    descripcion: str = Form(...),
    linea_id: Optional[str] = Form(None),
    segmento_id: Optional[str] = Form(None),
    precio_venta1_usd: float = Form(0),
    precio_venta2_usd: float = Form(0),
    precio_venta3_usd: float = Form(0),
    costo_producto_usd: float = Form(0),
    existencia: Optional[float] = Form(None),
    activo: Optional[str] = Form(None),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    accept_header = request.headers.get("accept", "")
    is_fetch = (
        request.headers.get("x-requested-with") == "fetch"
        or "application/json" in accept_header
        or request.headers.get("hx-request") == "true"
    )
    descripcion = descripcion.strip()
    if not descripcion:
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error=Faltan+datos+obligatorios", status_code=303)

    def _to_int(value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        return int(value) if value.isdigit() else None

    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if not producto:
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error=Producto+no+encontrado", status_code=303)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if not rate_today:
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error=Tasa+de+cambio+no+configurada", status_code=303)

    tasa = float(rate_today.rate)
    producto.descripcion = descripcion
    producto.linea_id = _to_int(linea_id)
    producto.segmento_id = _to_int(segmento_id)
    producto.precio_venta1_usd = precio_venta1_usd
    producto.precio_venta2_usd = precio_venta2_usd
    producto.precio_venta3_usd = precio_venta3_usd
    producto.precio_venta1 = precio_venta1_usd * tasa
    producto.precio_venta2 = precio_venta2_usd * tasa
    producto.precio_venta3 = precio_venta3_usd * tasa
    producto.costo_producto = costo_producto_usd * tasa
    producto.tasa_cambio = tasa
    producto.activo = activo == "on"

    if existencia is not None:
        if producto.saldo:
            producto.saldo.existencia = existencia
        else:
            db.add(SaldoProducto(producto_id=producto.id, existencia=existencia))

    db.commit()
    if is_fetch:
        return JSONResponse(
            {
                "ok": True,
                "message": "Producto actualizado exitosamente",
                "id": producto.id,
                "cod_producto": producto.cod_producto,
                "descripcion": producto.descripcion,
                "precio_venta1_usd": float(producto.precio_venta1_usd or 0),
                "precio_venta2_usd": float(producto.precio_venta2_usd or 0),
                "precio_venta3_usd": float(producto.precio_venta3_usd or 0),
                "costo_usd": float(costo_producto_usd or 0),
                "activo": bool(producto.activo),
            }
        )
    return RedirectResponse(redirect_to or "/inventory", status_code=303)


@router.get("/inventory/product/{product_id}/json")
def inventory_product_json(
    product_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if not producto:
        return JSONResponse({"ok": False, "message": "Producto no encontrado"}, status_code=404)
    tasa = float(producto.tasa_cambio or 0)
    costo_usd = float(producto.costo_producto or 0) / tasa if tasa else 0.0
    return JSONResponse(
        {
            "ok": True,
            "id": producto.id,
            "cod_producto": producto.cod_producto,
            "descripcion": producto.descripcion,
            "linea_id": producto.linea_id,
            "segmento_id": producto.segmento_id,
            "precio_venta1_usd": float(producto.precio_venta1_usd or 0),
            "precio_venta2_usd": float(producto.precio_venta2_usd or 0),
            "precio_venta3_usd": float(producto.precio_venta3_usd or 0),
            "costo_usd": costo_usd,
            "activo": bool(producto.activo),
        }
    )


@router.get("/inventory/product/{product_id}/combo")
def inventory_product_combo_list(
    product_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if not producto:
        return JSONResponse({"ok": False, "message": "Producto no encontrado"}, status_code=404)
    combos = (
        db.query(ProductoCombo)
        .filter(ProductoCombo.parent_producto_id == product_id)
        .order_by(ProductoCombo.id)
        .all()
    )
    items = []
    for combo in combos:
        child = combo.child
        items.append(
            {
                "id": combo.id,
                "child_id": child.id if child else None,
                "cod_producto": child.cod_producto if child else "",
                "descripcion": child.descripcion if child else "",
                "cantidad": float(combo.cantidad or 0),
                "precio_venta1_usd": float(child.precio_venta1_usd or 0) if child else 0.0,
                "precio_venta1": float(child.precio_venta1 or 0) if child else 0.0,
                "existencia": float(child.saldo.existencia or 0) if child and child.saldo else 0.0,
            }
        )
    return JSONResponse({"ok": True, "items": items})


@router.post("/inventory/product/{product_id}/combo")
def inventory_product_combo_add(
    product_id: int,
    child_id: int = Form(...),
    cantidad: float = Form(...),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if not producto:
        return JSONResponse({"ok": False, "message": "Producto no encontrado"}, status_code=404)
    child = db.query(Producto).filter(Producto.id == child_id).first()
    if not child:
        return JSONResponse({"ok": False, "message": "Regalia no encontrada"}, status_code=404)
    if cantidad <= 0:
        return JSONResponse({"ok": False, "message": "Cantidad invalida"}, status_code=400)
    existing = (
        db.query(ProductoCombo)
        .filter(
            ProductoCombo.parent_producto_id == product_id,
            ProductoCombo.child_producto_id == child_id,
        )
        .first()
    )
    if existing:
        existing.cantidad = cantidad
        existing.activo = True
    else:
        db.add(
            ProductoCombo(
                parent_producto_id=product_id,
                child_producto_id=child_id,
                cantidad=cantidad,
                activo=True,
            )
        )
    db.commit()
    return JSONResponse({"ok": True})


@router.post("/inventory/product/{product_id}/combo/{combo_id}/delete")
def inventory_product_combo_delete(
    product_id: int,
    combo_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    combo = (
        db.query(ProductoCombo)
        .filter(
            ProductoCombo.id == combo_id,
            ProductoCombo.parent_producto_id == product_id,
        )
        .first()
    )
    if not combo:
        return JSONResponse({"ok": False, "message": "Regalia no encontrada"}, status_code=404)
    db.delete(combo)
    db.commit()
    return JSONResponse({"ok": True})


@router.post("/inventory/linea")
def inventory_create_linea(
    cod_linea: str = Form(...),
    linea: str = Form(...),
    activo: Optional[str] = Form(None),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    exists = db.query(Linea).filter(Linea.cod_linea == cod_linea).first()
    if not exists:
        db.add(Linea(cod_linea=cod_linea, linea=linea, activo=activo == "on"))
        db.commit()
    return RedirectResponse(redirect_to or "/inventory", status_code=303)


@router.post("/inventory/linea/{linea_id}/update")
def inventory_update_linea(
    linea_id: int,
    linea: str = Form(...),
    activo: Optional[str] = Form(None),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    linea_obj = db.query(Linea).filter(Linea.id == linea_id).first()
    if linea_obj:
        linea_obj.linea = linea.strip()
        linea_obj.activo = activo == "on"
        db.commit()
    return RedirectResponse(redirect_to or "/inventory", status_code=303)


@router.post("/inventory/segmento")
def inventory_create_segmento(
    segmento: str = Form(...),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    exists = db.query(Segmento).filter(Segmento.segmento == segmento).first()
    if not exists:
        db.add(Segmento(segmento=segmento))
        db.commit()
    return RedirectResponse(redirect_to or "/inventory", status_code=303)


@router.post("/inventory/segmento/{segmento_id}/update")
def inventory_update_segmento(
    segmento_id: int,
    segmento: str = Form(...),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    segmento_obj = db.query(Segmento).filter(Segmento.id == segmento_id).first()
    if segmento_obj:
        segmento_obj.segmento = segmento.strip()
        db.commit()
    return RedirectResponse(redirect_to or "/inventory", status_code=303)


@router.post("/inventory/proveedor")
def inventory_create_proveedor(
    request: Request,
    nombre: str = Form(...),
    tipo: Optional[str] = Form(None),
    activo: Optional[str] = Form(None),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        if request.headers.get("X-Requested-With") == "fetch":
            return JSONResponse({"ok": False, "message": "Nombre requerido"}, status_code=400)
        return RedirectResponse(redirect_to or "/inventory/ingresos", status_code=303)
    exists = db.query(Proveedor).filter(func.lower(Proveedor.nombre) == nombre.lower()).first()
    if not exists:
        proveedor = Proveedor(nombre=nombre, tipo=tipo, activo=activo == "on")
        db.add(proveedor)
        db.commit()
        if request.headers.get("X-Requested-With") == "fetch":
            return JSONResponse(
                {
                    "ok": True,
                    "id": proveedor.id,
                    "nombre": proveedor.nombre,
                    "tipo": proveedor.tipo,
                    "activo": proveedor.activo,
                }
            )
    if request.headers.get("X-Requested-With") == "fetch":
        return JSONResponse({"ok": False, "message": "Proveedor ya existe"}, status_code=409)
    return RedirectResponse(redirect_to or "/inventory/ingresos", status_code=303)


@router.post("/inventory/proveedor/{proveedor_id}/update")
def inventory_update_proveedor(
    request: Request,
    proveedor_id: int,
    nombre: str = Form(...),
    tipo: Optional[str] = Form(None),
    activo: Optional[str] = Form(None),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    proveedor_obj = db.query(Proveedor).filter(Proveedor.id == proveedor_id).first()
    if proveedor_obj:
        proveedor_obj.nombre = nombre.strip()
        proveedor_obj.tipo = tipo
        proveedor_obj.activo = activo == "on"
        db.commit()
        if request.headers.get("X-Requested-With") == "fetch":
            return JSONResponse(
                {
                    "ok": True,
                    "id": proveedor_obj.id,
                    "nombre": proveedor_obj.nombre,
                    "tipo": proveedor_obj.tipo,
                    "activo": proveedor_obj.activo,
                }
            )
    if request.headers.get("X-Requested-With") == "fetch":
        return JSONResponse({"ok": False, "message": "Proveedor no encontrado"}, status_code=404)
    return RedirectResponse(redirect_to or "/inventory/ingresos", status_code=303)


@router.post("/inventory/ingresos")
async def inventory_create_ingreso(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    tipo_id = form.get("tipo_id")
    bodega_id = form.get("bodega_id")
    proveedor_id = form.get("proveedor_id") or None
    fecha = form.get("fecha")
    moneda = form.get("moneda")
    observacion = form.get("observacion") or None
    item_ids = form.getlist("item_producto_id")
    item_qtys = form.getlist("item_cantidad")
    item_costs = form.getlist("item_costo")
    item_prices = form.getlist("item_precio")

    if not tipo_id or not bodega_id or not fecha or not moneda:
        return RedirectResponse("/inventory/ingresos?error=Faltan+datos+obligatorios", status_code=303)
    if not item_ids:
        return RedirectResponse("/inventory/ingresos?error=Agrega+productos+al+ingreso", status_code=303)

    tipo = db.query(IngresoTipo).filter(IngresoTipo.id == int(tipo_id)).first()
    if not tipo:
        return RedirectResponse("/inventory/ingresos?error=Tipo+no+valido", status_code=303)
    if tipo.requiere_proveedor and not proveedor_id:
        return RedirectResponse("/inventory/ingresos?error=Proveedor+requerido", status_code=303)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if moneda == "USD" and not rate_today:
        return RedirectResponse("/inventory/ingresos?error=Tasa+de+cambio+no+configurada", status_code=303)

    def to_float(value: Optional[str]) -> float:
        if not value:
            return 0.0
        try:
            return float(value)
        except ValueError:
            return 0.0

    def to_decimal(value: Optional[float]) -> Decimal:
        return Decimal(str(value or 0))

    tasa = float(rate_today.rate) if rate_today else 0
    fecha_value = date.fromisoformat(str(fecha).split("T")[0])
    ingreso = IngresoInventario(
        tipo_id=int(tipo_id),
        bodega_id=int(bodega_id),
        proveedor_id=int(proveedor_id) if proveedor_id else None,
        fecha=fecha_value,
        moneda=moneda,
        tasa_cambio=tasa if moneda == "USD" else None,
        observacion=observacion,
        usuario_registro=user.full_name,
    )
    db.add(ingreso)
    db.flush()

    total_usd = 0.0
    total_cs = 0.0
    for index, product_id in enumerate(item_ids):
        qty = to_float(item_qtys[index] if index < len(item_qtys) else 0)
        cost = to_float(item_costs[index] if index < len(item_costs) else 0)
        price = to_float(item_prices[index] if index < len(item_prices) else 0)
        if qty <= 0:
            continue
        qty_dec = to_decimal(qty)

        if moneda == "USD":
            costo_usd = cost
            costo_cs = cost * tasa
            precio_usd = price
            precio_cs = price * tasa
        else:
            costo_cs = cost
            costo_usd = cost / tasa if tasa else 0
            precio_cs = price
            precio_usd = price / tasa if tasa else 0

        subtotal_usd = costo_usd * qty
        subtotal_cs = costo_cs * qty
        total_usd += subtotal_usd
        total_cs += subtotal_cs

        item = IngresoItem(
            ingreso_id=ingreso.id,
            producto_id=int(product_id),
            cantidad=qty,
            costo_unitario_usd=costo_usd,
            costo_unitario_cs=costo_cs,
            subtotal_usd=subtotal_usd,
            subtotal_cs=subtotal_cs,
        )
        db.add(item)

        producto = db.query(Producto).filter(Producto.id == int(product_id)).first()
        if producto:
            if producto.saldo:
                current = Decimal(str(producto.saldo.existencia or 0))
                producto.saldo.existencia = current + qty_dec
            else:
                db.add(SaldoProducto(producto_id=producto.id, existencia=qty_dec))
            if cost > 0:
                producto.costo_producto = costo_cs
            if price > 0:
                producto.precio_venta1 = precio_cs
                if precio_usd > 0:
                    producto.precio_venta1_usd = precio_usd

    ingreso.total_usd = total_usd
    ingreso.total_cs = total_cs
    db.commit()
    return RedirectResponse(
        f"/inventory/ingresos?success=Ingreso+registrado&print_id={ingreso.id}",
        status_code=303,
    )


@router.post("/inventory/egresos")
async def inventory_create_egreso(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    tipo_id = form.get("tipo_id")
    bodega_id = form.get("bodega_id")
    fecha = form.get("fecha")
    moneda = form.get("moneda")
    observacion = form.get("observacion") or None
    item_ids = form.getlist("item_producto_id")
    item_qtys = form.getlist("item_cantidad")
    item_costs = form.getlist("item_costo")
    item_prices = form.getlist("item_precio")

    if not tipo_id or not bodega_id or not fecha or not moneda:
        return RedirectResponse("/inventory/egresos?error=Faltan+datos+obligatorios", status_code=303)
    if not item_ids:
        return RedirectResponse("/inventory/egresos?error=Agrega+productos+al+egreso", status_code=303)

    tipo = db.query(EgresoTipo).filter(EgresoTipo.id == int(tipo_id)).first()
    if not tipo:
        return RedirectResponse("/inventory/egresos?error=Tipo+no+valido", status_code=303)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if moneda == "USD" and not rate_today:
        return RedirectResponse("/inventory/egresos?error=Tasa+de+cambio+no+configurada", status_code=303)

    def to_float(value: Optional[str]) -> float:
        if not value:
            return 0.0
        try:
            return float(value)
        except ValueError:
            return 0.0

    tasa = float(rate_today.rate) if rate_today else 0
    fecha_value = date.fromisoformat(str(fecha).split("T")[0])
    egreso = EgresoInventario(
        tipo_id=int(tipo_id),
        bodega_id=int(bodega_id),
        fecha=fecha_value,
        moneda=moneda,
        tasa_cambio=tasa if moneda == "USD" else None,
        observacion=observacion,
        usuario_registro=user.full_name,
    )
    db.add(egreso)
    db.flush()

    total_usd = 0.0
    total_cs = 0.0
    for index, product_id in enumerate(item_ids):
        qty = to_float(item_qtys[index] if index < len(item_qtys) else 0)
        cost = to_float(item_costs[index] if index < len(item_costs) else 0)
        if qty <= 0:
            continue
        producto = db.query(Producto).filter(Producto.id == int(product_id)).first()
        if not producto:
            db.rollback()
            return RedirectResponse("/inventory/egresos?error=Producto+no+encontrado", status_code=303)

        existencia = float(producto.saldo.existencia or 0) if producto.saldo else 0.0
        if existencia < qty:
            db.rollback()
            mensaje = f"Stock+insuficiente+para+{producto.cod_producto}"
            return RedirectResponse(f"/inventory/egresos?error={mensaje}", status_code=303)

        if moneda == "USD":
            costo_usd = cost
            costo_cs = cost * tasa
        else:
            costo_cs = cost
            costo_usd = cost / tasa if tasa else 0

        subtotal_usd = costo_usd * qty
        subtotal_cs = costo_cs * qty
        total_usd += subtotal_usd
        total_cs += subtotal_cs

        item = EgresoItem(
            egreso_id=egreso.id,
            producto_id=int(product_id),
            cantidad=qty,
            costo_unitario_usd=costo_usd,
            costo_unitario_cs=costo_cs,
            subtotal_usd=subtotal_usd,
            subtotal_cs=subtotal_cs,
        )
        db.add(item)

        if producto.saldo:
            existencia_actual = float(producto.saldo.existencia or 0)
            producto.saldo.existencia = existencia_actual - qty

    egreso.total_usd = total_usd
    egreso.total_cs = total_cs
    db.commit()
    return RedirectResponse(
        f"/inventory/egresos?success=Egreso+registrado&print_id={egreso.id}",
        status_code=303,
    )


@router.post("/sales/cliente")
def sales_create_cliente(
    request: Request,
    nombre: str = Form(...),
    identificacion: Optional[str] = Form(None),
    telefono: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    direccion: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        return JSONResponse({"ok": False, "message": "Nombre requerido"}, status_code=400)
    exists = db.query(Cliente).filter(func.lower(Cliente.nombre) == nombre.lower()).first()
    if exists:
        return JSONResponse({"ok": False, "message": "Cliente ya existe"}, status_code=409)
    cliente = Cliente(
        nombre=nombre,
        identificacion=identificacion.strip() if identificacion else None,
        telefono=telefono,
        email=email,
        direccion=direccion,
        activo=True,
    )
    db.add(cliente)
    db.commit()
    return JSONResponse({"ok": True, "id": cliente.id, "nombre": cliente.nombre})


@router.post("/sales/cliente/{cliente_id}/update")
def sales_update_cliente(
    cliente_id: int,
    nombre: str = Form(...),
    identificacion: Optional[str] = Form(None),
    telefono: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
    direccion: Optional[str] = Form(None),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    cliente = db.query(Cliente).filter(Cliente.id == cliente_id).first()
    if not cliente:
        return JSONResponse({"ok": False, "message": "Cliente no encontrado"}, status_code=404)
    cliente.nombre = nombre.strip()
    cliente.identificacion = identificacion.strip() if identificacion else None
    cliente.telefono = telefono
    cliente.email = email
    cliente.direccion = direccion
    cliente.activo = activo == "on"
    db.commit()
    return JSONResponse({"ok": True, "id": cliente.id, "nombre": cliente.nombre})


@router.post("/sales/vendedor")
def sales_create_vendedor(
    nombre: str = Form(...),
    telefono: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    nombre = nombre.strip()
    if not nombre:
        return JSONResponse({"ok": False, "message": "Nombre requerido"}, status_code=400)
    exists = db.query(Vendedor).filter(func.lower(Vendedor.nombre) == nombre.lower()).first()
    if exists:
        return JSONResponse({"ok": False, "message": "Vendedor ya existe"}, status_code=409)
    vendedor = Vendedor(nombre=nombre, telefono=telefono, activo=True)
    db.add(vendedor)
    db.commit()
    return JSONResponse({"ok": True, "id": vendedor.id, "nombre": vendedor.nombre})


@router.post("/sales/vendedor/{vendedor_id}/update")
def sales_update_vendedor(
    vendedor_id: int,
    nombre: str = Form(...),
    telefono: Optional[str] = Form(None),
    activo: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    vendedor = db.query(Vendedor).filter(Vendedor.id == vendedor_id).first()
    if not vendedor:
        return JSONResponse({"ok": False, "message": "Vendedor no encontrado"}, status_code=404)
    vendedor.nombre = nombre.strip()
    vendedor.telefono = telefono
    vendedor.activo = activo == "on"
    db.commit()
    return JSONResponse({"ok": True, "id": vendedor.id, "nombre": vendedor.nombre})


@router.post("/sales")
async def sales_create_invoice(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    form = await request.form()
    cliente_id = form.get("cliente_id") or None
    vendedor_id = form.get("vendedor_id") or None
    fecha = form.get("fecha")
    moneda = form.get("moneda")
    forma_pago_id = form.get("forma_pago_id") or None
    banco_id = form.get("banco_id") or None
    cuenta_id = form.get("cuenta_id") or None
    pago_monto = form.get("pago_monto") or None
    pago_forma_ids = form.getlist("pago_forma_id")
    pago_monedas = form.getlist("pago_moneda")
    pago_montos = form.getlist("pago_monto")
    pago_banco_ids = form.getlist("pago_banco_id")
    pago_cuenta_ids = form.getlist("pago_cuenta_id")
    item_ids = form.getlist("item_producto_id")
    item_qtys = form.getlist("item_cantidad")
    item_prices = form.getlist("item_precio")
    item_roles = form.getlist("item_role")
    item_combo_groups = form.getlist("item_combo_group")

    if not fecha:
        fecha = local_today().isoformat()
    if not moneda:
        moneda = "CS"
    if not vendedor_id:
        vendedor = db.query(Vendedor).filter(Vendedor.activo.is_(True)).order_by(Vendedor.id).first()
        if vendedor:
            vendedor_id = str(vendedor.id)
    if not vendedor_id or not fecha or not moneda:
        return RedirectResponse("/sales?error=Faltan+datos+obligatorios", status_code=303)
    if not item_ids:
        return RedirectResponse("/sales?error=Agrega+productos+a+la+venta", status_code=303)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if moneda == "USD" and not rate_today:
        return RedirectResponse("/sales?error=Tasa+de+cambio+no+configurada", status_code=303)

    def to_float(value: Optional[str]) -> float:
        if not value:
            return 0.0
        try:
            return float(value)
        except ValueError:
            return 0.0

    def to_decimal(value: Optional[float]) -> Decimal:
        return Decimal(str(value or 0))

    tasa = float(rate_today.rate) if rate_today else 0
    branch, bodega = _resolve_branch_bodega(db, user)
    if not branch:
        return RedirectResponse("/sales?error=Usuario+sin+sucursal+asignada", status_code=303)
    if not bodega:
        return RedirectResponse("/sales?error=Bodega+no+configurada+para+la+sucursal", status_code=303)
    last_factura = (
        db.query(VentaFactura)
        .filter(VentaFactura.bodega_id == bodega.id)
        .order_by(VentaFactura.secuencia.desc())
        .first()
    )
    next_seq = (last_factura.secuencia if last_factura else 0) + 1
    branch_code = (branch.code or "").lower()
    prefix = "C" if branch_code == "central" else "E" if branch_code == "esteli" else branch_code[:1].upper()
    width = 6
    numero = f"{prefix}-{next_seq:0{width}d}"

    now_local = local_now()
    try:
        fecha_value = date.fromisoformat(str(fecha).split("T")[0])
    except (TypeError, ValueError):
        fecha_value = local_today()
    fecha_dt = datetime.combine(fecha_value, now_local.time()).replace(tzinfo=None)
    factura = VentaFactura(
        secuencia=next_seq,
        numero=numero,
        bodega_id=bodega.id,
        cliente_id=int(cliente_id) if cliente_id else None,
        vendedor_id=int(vendedor_id) if vendedor_id else None,
        fecha=fecha_dt,
        moneda=moneda,
        tasa_cambio=tasa if moneda == "USD" else None,
        usuario_registro=user.full_name,
        created_at=local_now_naive(),
    )
    db.add(factura)
    db.flush()

    total_usd = 0.0
    total_cs = 0.0
    total_items = 0.0
    product_ids = [int(pid) for pid in item_ids if str(pid).isdigit()]
    balances = _balances_by_bodega(db, [bodega.id], list(set(product_ids))) if product_ids else {}
    for index, product_id in enumerate(item_ids):
        qty = to_float(item_qtys[index] if index < len(item_qtys) else 0)
        price = to_float(item_prices[index] if index < len(item_prices) else 0)
        if qty <= 0:
            continue

        producto = db.query(Producto).filter(Producto.id == int(product_id)).first()
        if not producto:
            db.rollback()
            return RedirectResponse("/sales?error=Producto+no+encontrado", status_code=303)

        existencia = float(balances.get((producto.id, bodega.id), Decimal("0")) or 0)
        if existencia < qty:
            db.rollback()
            mensaje = f"Stock+insuficiente+para+{producto.cod_producto}"
            return RedirectResponse(f"/sales?error={mensaje}", status_code=303)
        balances[(producto.id, bodega.id)] = Decimal(str(existencia)) - to_decimal(qty)

        if moneda == "USD":
            precio_usd = price
            precio_cs = price * tasa
        else:
            precio_cs = price
            precio_usd = price / tasa if tasa else 0

        subtotal_usd = precio_usd * qty
        subtotal_cs = precio_cs * qty
        total_usd += subtotal_usd
        total_cs += subtotal_cs
        total_items += qty

        combo_role = item_roles[index] if index < len(item_roles) else None
        combo_group = item_combo_groups[index] if index < len(item_combo_groups) else None
        combo_role = combo_role or None
        combo_group = combo_group or None
        item = VentaItem(
            factura_id=factura.id,
            producto_id=int(product_id),
            cantidad=qty,
            precio_unitario_usd=precio_usd,
            precio_unitario_cs=precio_cs,
            subtotal_usd=subtotal_usd,
            subtotal_cs=subtotal_cs,
            combo_role=combo_role,
            combo_group=combo_group,
        )
        db.add(item)

        # No usar saldo global; el stock se calcula por movimientos/bodega.

    factura.total_usd = total_usd
    factura.total_cs = total_cs
    factura.total_items = total_items

    pagos = []
    if pago_forma_ids:
        for index, forma_id in enumerate(pago_forma_ids):
            moneda_pago = pago_monedas[index] if index < len(pago_monedas) else moneda
            monto_pago = to_float(pago_montos[index] if index < len(pago_montos) else 0)
            banco_pago = pago_banco_ids[index] if index < len(pago_banco_ids) else None
            cuenta_pago = pago_cuenta_ids[index] if index < len(pago_cuenta_ids) else None
            if monto_pago <= 0:
                continue
            if moneda_pago != moneda and not tasa:
                db.rollback()
                return RedirectResponse("/sales?error=Tasa+de+cambio+no+configurada", status_code=303)
            pago_usd = monto_pago if moneda_pago == "USD" else monto_pago / tasa if tasa else 0
            pago_cs = monto_pago if moneda_pago == "CS" else monto_pago * tasa
            pagos.append(
                VentaPago(
                    factura_id=factura.id,
                    forma_pago_id=int(forma_id),
                    banco_id=int(banco_pago) if banco_pago else None,
                    cuenta_id=int(cuenta_pago) if cuenta_pago else None,
                    monto_usd=pago_usd,
                    monto_cs=pago_cs,
                )
            )
    elif forma_pago_id:
        monto_pago = to_float(pago_monto) if pago_monto is not None else (total_usd if moneda == "USD" else total_cs)
        pago_usd = monto_pago if moneda == "USD" else monto_pago / tasa if tasa else 0
        pago_cs = monto_pago if moneda == "CS" else monto_pago * tasa
        pagos.append(
            VentaPago(
                factura_id=factura.id,
                forma_pago_id=int(forma_pago_id),
                banco_id=int(banco_id) if banco_id else None,
                cuenta_id=int(cuenta_id) if cuenta_id else None,
                monto_usd=pago_usd,
                monto_cs=pago_cs,
            )
        )

    if not pagos:
        db.rollback()
        return RedirectResponse("/sales?error=Agrega+pagos+para+registrar", status_code=303)

    total_paid = sum(pago.monto_usd for pago in pagos) if moneda == "USD" else sum(pago.monto_cs for pago in pagos)
    due_total = total_usd if moneda == "USD" else total_cs
    if float(total_paid) < float(due_total):
        db.rollback()
        return RedirectResponse("/sales?error=Pago+incompleto", status_code=303)

    factura.estado_cobranza = "PAGADA"
    for pago in pagos:
        db.add(pago)

    db.commit()
    pos_print = (
        db.query(PosPrintSetting)
        .filter(PosPrintSetting.branch_id == branch.id)
        .first()
    )
    if pos_print and pos_print.auto_print:
        try:
            _print_pos_ticket(
                factura,
                pos_print.printer_name,
                pos_print.copies,
                pos_print.sumatra_path,
            )
        except Exception:
            pass
    return RedirectResponse(
        f"/sales?success=Venta+registrada&print_id={factura.id}",
        status_code=303,
    )


@router.get("/sales/cobranza")
def sales_cobranza(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    _enforce_permission(request, user, "access.sales.cobranza")
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    producto_q = (request.query_params.get("producto") or "").strip()
    vendedor_q = (request.query_params.get("vendedor_id") or "").strip()
    today_value = local_today()
    start_date = today_value
    end_date = today_value
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today_value
            end_date = today_value

    _, bodega = _resolve_branch_bodega(db, user)
    ventas_query = db.query(VentaFactura)
    if bodega:
        ventas_query = ventas_query.filter(VentaFactura.bodega_id == bodega.id)
    ventas_query = ventas_query.filter(VentaFactura.fecha >= start_date, VentaFactura.fecha <= end_date)
    if vendedor_q:
        try:
            ventas_query = ventas_query.filter(VentaFactura.vendedor_id == int(vendedor_q))
        except ValueError:
            pass
    if producto_q:
        ventas_query = ventas_query.join(VentaItem).join(Producto).filter(
            or_(
                func.lower(Producto.cod_producto).like(f"%{producto_q.lower()}%"),
                func.lower(Producto.descripcion).like(f"%{producto_q.lower()}%"),
            )
        )
    ventas = ventas_query.order_by(VentaFactura.fecha.desc(), VentaFactura.id.desc()).all()

    results = []
    total_saldo_cs = Decimal("0")
    total_saldo_usd = Decimal("0")
    for factura in ventas:
        if factura.estado == "ANULADA":
            total_abono_usd = Decimal("0")
            total_abono_cs = Decimal("0")
            saldo_usd = Decimal("0")
            saldo_cs = Decimal("0")
        else:
            total_abono_usd = sum(Decimal(str(a.monto_usd or 0)) for a in factura.abonos)
            total_abono_cs = sum(Decimal(str(a.monto_cs or 0)) for a in factura.abonos)
            total_due_usd = Decimal(str(factura.total_usd or 0))
            total_due_cs = Decimal(str(factura.total_cs or 0))

            if factura.estado_cobranza == "PENDIENTE":
                total_paid_usd = total_abono_usd
                total_paid_cs = total_abono_cs
            else:
                total_paid_usd = sum(Decimal(str(p.monto_usd or 0)) for p in factura.pagos) + total_abono_usd
                total_paid_cs = sum(Decimal(str(p.monto_cs or 0)) for p in factura.pagos) + total_abono_cs

            saldo_usd = max(total_due_usd - total_paid_usd, Decimal("0"))
            saldo_cs = max(total_due_cs - total_paid_cs, Decimal("0"))
            total_saldo_cs += saldo_cs
            total_saldo_usd += saldo_usd
        results.append(
            {
                "factura": factura,
                "abonos_usd": total_abono_usd,
                "abonos_cs": total_abono_cs,
                "saldo_usd": saldo_usd,
                "saldo_cs": saldo_cs,
            }
        )

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")
    vendedores = _vendedores_for_bodega(db, bodega)

    return request.app.state.templates.TemplateResponse(
        "sales_cobranza.html",
        {
            "request": request,
            "user": user,
            "ventas": results,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "producto_q": producto_q,
            "vendedor_q": vendedor_q,
            "vendedores": vendedores,
            "default_vendedor_id": _default_vendedor_id(db, bodega),
            "total_saldo_cs": float(total_saldo_cs),
            "total_saldo_usd": float(total_saldo_usd),
            "version": settings.UI_VERSION,
        },
    )


@router.get("/sales/cobranza/export")
def sales_cobranza_export(
    request: Request,
    format: str = "pdf",
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    start_raw = request.query_params.get("start_date")
    end_raw = request.query_params.get("end_date")
    producto_q = (request.query_params.get("producto") or "").strip()
    vendedor_q = (request.query_params.get("vendedor_id") or "").strip()
    today_value = local_today()
    start_date = today_value
    end_date = today_value
    if start_raw or end_raw:
        try:
            if start_raw:
                start_date = date.fromisoformat(start_raw)
            if end_raw:
                end_date = date.fromisoformat(end_raw)
        except ValueError:
            start_date = today_value
            end_date = today_value

    _, bodega = _resolve_branch_bodega(db, user)
    ventas_query = db.query(VentaFactura)
    if bodega:
        ventas_query = ventas_query.filter(VentaFactura.bodega_id == bodega.id)
    ventas_query = ventas_query.filter(VentaFactura.fecha >= start_date, VentaFactura.fecha <= end_date)
    if vendedor_q:
        try:
            ventas_query = ventas_query.filter(VentaFactura.vendedor_id == int(vendedor_q))
        except ValueError:
            pass
    if producto_q:
        ventas_query = ventas_query.join(VentaItem).join(Producto).filter(
            or_(
                func.lower(Producto.cod_producto).like(f"%{producto_q.lower()}%"),
                func.lower(Producto.descripcion).like(f"%{producto_q.lower()}%"),
            )
        )
    ventas = ventas_query.order_by(VentaFactura.fecha.desc(), VentaFactura.id.desc()).all()

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

    rows = []
    resumen = {}
    total_saldo_cs = Decimal("0")
    total_saldo_usd = Decimal("0")
    for factura in ventas:
        if factura.estado == "ANULADA":
            total_abono_usd = Decimal("0")
            total_abono_cs = Decimal("0")
            saldo_usd = Decimal("0")
            saldo_cs = Decimal("0")
        else:
            total_abono_usd = sum(Decimal(str(a.monto_usd or 0)) for a in factura.abonos)
            total_abono_cs = sum(Decimal(str(a.monto_cs or 0)) for a in factura.abonos)
            total_due_usd = Decimal(str(factura.total_usd or 0))
            total_due_cs = Decimal(str(factura.total_cs or 0))
            if factura.estado_cobranza == "PENDIENTE":
                total_paid_usd = total_abono_usd
                total_paid_cs = total_abono_cs
            else:
                total_paid_usd = sum(Decimal(str(p.monto_usd or 0)) for p in factura.pagos) + total_abono_usd
                total_paid_cs = sum(Decimal(str(p.monto_cs or 0)) for p in factura.pagos) + total_abono_cs
            saldo_usd = max(total_due_usd - total_paid_usd, Decimal("0"))
            saldo_cs = max(total_due_cs - total_paid_cs, Decimal("0"))
            total_saldo_cs += saldo_cs
            total_saldo_usd += saldo_usd
        vendedor_name = factura.vendedor.nombre if factura.vendedor else "-"
        resumen.setdefault(vendedor_name, {"cs": Decimal("0"), "usd": Decimal("0")})
        resumen[vendedor_name]["cs"] += saldo_cs
        resumen[vendedor_name]["usd"] += saldo_usd
        rows.append(
            {
                "numero": factura.numero,
                "fecha": factura.fecha,
                "cliente": factura.cliente.nombre if factura.cliente else "Consumidor final",
                "vendedor": vendedor_name,
                "saldo_usd": saldo_usd,
                "saldo_cs": saldo_cs,
                "estado": factura.estado_cobranza,
            }
        )

    if format.lower() != "pdf":
        return JSONResponse({"ok": False, "message": "Formato no soportado"}, status_code=400)

    buffer = io.BytesIO()
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter

    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    y = height - 40
    logo_path = Path(__file__).resolve().parent.parent / "static" / "logo_hollywood.png"
    if logo_path.exists():
        c.drawImage(str(logo_path), 40, y - 30, width=90, height=30, mask="auto")
    c.setFont("Times-Bold", 14)
    c.drawString(150, y - 8, "Informe de Cuentas por Cobrar")
    y -= 35
    c.setFont("Times-Roman", 10)
    branch_name = "-"
    if bodega and bodega.branch:
        branch_name = bodega.branch.name
    c.drawString(40, y, f"Sucursal: {branch_name}")
    y -= 14
    c.drawString(40, y, f"Rango: {start_date} a {end_date}")
    y -= 14
    if vendedor_q:
        vendedor_name = next((v.nombre for v in db.query(Vendedor).all() if str(v.id) == vendedor_q), "")
        c.drawString(40, y, f"Vendedor: {vendedor_name}")
        y -= 14
    c.drawString(40, y, f"Tasa: {rate_today.rate if rate_today else 'N/D'}")
    y -= 16
    c.line(40, y, width - 40, y)
    y -= 16

    c.setFont("Times-Bold", 11)
    c.drawString(40, y, "Resumen por vendedor")
    y -= 12
    c.setFont("Times-Bold", 9)
    c.drawString(50, y, "Vendedor")
    c.drawString(320, y, "Saldo C$")
    c.drawString(430, y, "Saldo $")
    y -= 8
    c.line(40, y, width - 40, y)
    y -= 10
    c.setFont("Times-Roman", 9)
    resumen_total_cs = Decimal("0")
    resumen_total_usd = Decimal("0")
    for vend, totals in resumen.items():
        if y < 120:
            c.showPage()
            y = height - 60
        c.drawString(50, y, vend)
        c.setFillColor(colors.HexColor("#1d4ed8"))
        c.drawRightString(400, y, f"C$ {totals['cs']:,.2f}")
        c.setFillColor(colors.HexColor("#16a34a"))
        c.drawRightString(500, y, f"$ {totals['usd']:,.2f}")
        c.setFillColor(colors.black)
        resumen_total_cs += totals["cs"]
        resumen_total_usd += totals["usd"]
        y -= 12

    y -= 4
    c.setFillColor(colors.HexColor("#1e3a8a"))
    c.roundRect(40, y - 6, width - 80, 12, 4, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Times-Bold", 9)
    c.drawString(50, y - 2, "Total resumen")
    c.drawRightString(400, y - 2, f"C$ {resumen_total_cs:,.2f}")
    c.drawRightString(500, y - 2, f"$ {resumen_total_usd:,.2f}")
    c.setFillColor(colors.black)
    y -= 18

    c.setFillColor(colors.HexColor("#1e3a8a"))
    c.roundRect(40, y - 6, width - 80, 12, 4, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Times-Bold", 9)
    c.drawString(50, y - 2, "Detalle de creditos")
    c.setFillColor(colors.black)
    y -= 18

    c.setFillColor(colors.black)
    c.setFont("Times-Bold", 11)
    c.setFont("Times-Bold", 9)
    c.drawString(40, y, "Factura")
    c.drawString(110, y, "Cliente")
    c.drawString(260, y, "Vendedor")
    c.drawString(370, y, "Estado")
    c.drawString(430, y, "Saldo C$")
    c.drawString(510, y, "Saldo $")
    y -= 8
    c.line(40, y, width - 40, y)
    y -= 12
    c.setFont("Times-Roman", 9)
    for row in rows:
        if y < 80:
            c.showPage()
            y = height - 60
        c.drawString(40, y, row["numero"])
        c.drawString(110, y, (row["cliente"][:20] + "…") if len(row["cliente"]) > 20 else row["cliente"])
        c.drawString(260, y, (row["vendedor"][:14] + "…") if len(row["vendedor"]) > 14 else row["vendedor"])
        c.drawString(370, y, row["estado"])
        c.setFillColor(colors.HexColor("#1d4ed8"))
        c.drawRightString(495, y, f"C$ {row['saldo_cs']:,.2f}")
        c.setFillColor(colors.HexColor("#16a34a"))
        c.drawRightString(570, y, f"$ {row['saldo_usd']:,.2f}")
        c.setFillColor(colors.black)
        y -= 12

    y -= 4
    c.line(40, y, width - 40, y)
    y -= 14
    c.setFont("Times-Bold", 10)
    c.drawString(40, y, "Totales pendientes:")
    c.setFillColor(colors.HexColor("#1d4ed8"))
    c.drawRightString(495, y, f"C$ {total_saldo_cs:,.2f}")
    c.setFillColor(colors.HexColor("#16a34a"))
    c.drawRightString(570, y, f"$ {total_saldo_usd:,.2f}")
    c.setFillColor(colors.black)
    y -= 14
    c.showPage()
    c.save()
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=cobranza.pdf"},
    )


@router.get("/sales/cobranza/{venta_id}/abonos")
def sales_cobranza_abonos(
    venta_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    factura = db.query(VentaFactura).filter(VentaFactura.id == venta_id).first()
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    if factura.estado == "ANULADA":
        return JSONResponse({"ok": False, "message": "Factura anulada"}, status_code=400)
    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and factura.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Factura fuera de tu bodega"}, status_code=403)
    abonos = (
        db.query(CobranzaAbono)
        .filter(CobranzaAbono.factura_id == factura.id)
        .order_by(CobranzaAbono.fecha.desc(), CobranzaAbono.id.desc())
        .all()
    )
    items = []
    for abono in abonos:
        items.append(
            {
                "id": abono.id,
                "numero": abono.numero,
                "fecha": abono.fecha.isoformat() if abono.fecha else "",
                "moneda": abono.moneda,
                "monto": float(abono.monto_usd if abono.moneda == "USD" else abono.monto_cs),
                "observacion": abono.observacion or "",
            }
        )
    return JSONResponse({"ok": True, "items": items})


@router.post("/sales/cobranza/{venta_id}/abono")
async def sales_cobranza_abono(
    venta_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    form = await request.form()
    moneda = (form.get("moneda") or "CS").upper()
    monto_raw = form.get("monto")
    observacion = (form.get("observacion") or "").strip()
    if not monto_raw:
        return JSONResponse({"ok": False, "message": "Monto requerido"}, status_code=400)
    if moneda not in {"CS", "USD"}:
        return JSONResponse({"ok": False, "message": "Moneda invalida"}, status_code=400)

    def parse_decimal(value: str) -> Decimal:
        raw = re.sub(r"[^0-9.,-]", "", str(value or "0"))
        if "," in raw and "." in raw:
            if raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "," in raw and "." not in raw:
            parts = raw.split(",")
            if len(parts) == 2 and len(parts[1]) == 2:
                raw = raw.replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "." in raw and "," not in raw:
            parts = raw.split(".")
            if len(parts) == 2 and len(parts[1]) == 2:
                raw = raw
            else:
                raw = raw.replace(".", "")
        try:
            return Decimal(raw)
        except Exception:
            return Decimal("0")

    monto = parse_decimal(monto_raw)
    if monto <= 0:
        return JSONResponse({"ok": False, "message": "Monto invalido"}, status_code=400)

    factura = db.query(VentaFactura).filter(VentaFactura.id == venta_id).first()
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    if factura.estado == "ANULADA":
        return JSONResponse({"ok": False, "message": "Factura anulada"}, status_code=400)
    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and factura.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Factura fuera de tu bodega"}, status_code=403)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if moneda == "USD" and not rate_today:
        return JSONResponse({"ok": False, "message": "Tasa no configurada"}, status_code=400)
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

    if moneda == "USD":
        monto_usd = monto
        monto_cs = monto * tasa
    else:
        monto_cs = monto
        monto_usd = monto / tasa if tasa else Decimal("0")

    if factura.estado_cobranza == "PAGADA":
        return JSONResponse({"ok": False, "message": "Factura ya pagada"}, status_code=400)

    last_abono = (
        db.query(CobranzaAbono)
        .filter(CobranzaAbono.bodega_id == factura.bodega_id)
        .order_by(CobranzaAbono.secuencia.desc())
        .first()
    )
    next_seq = (last_abono.secuencia if last_abono else 0) + 1
    branch_code = (factura.bodega.branch.code if factura.bodega and factura.bodega.branch else "").lower()
    prefix = "C" if branch_code == "central" else "E" if branch_code == "esteli" else (branch_code[:1].upper() or "X")
    numero = f"RE-{prefix}-{next_seq:06d}"

    abono = CobranzaAbono(
        factura_id=factura.id,
        branch_id=factura.bodega.branch_id if factura.bodega else user.default_branch_id,
        bodega_id=factura.bodega_id or user.default_bodega_id,
        secuencia=next_seq,
        numero=numero,
        fecha=local_today(),
        moneda=moneda,
        tasa_cambio=tasa if tasa else None,
        monto_usd=monto_usd,
        monto_cs=monto_cs,
        observacion=observacion,
        usuario_registro=user.full_name,
    )
    db.add(abono)

    total_paid_usd = sum(Decimal(str(a.monto_usd or 0)) for a in factura.abonos) + monto_usd
    total_paid_cs = sum(Decimal(str(a.monto_cs or 0)) for a in factura.abonos) + monto_cs
    due_usd = Decimal(str(factura.total_usd or 0))
    due_cs = Decimal(str(factura.total_cs or 0))
    if (factura.moneda or "CS") == "USD":
        factura.estado_cobranza = "PAGADA" if total_paid_usd >= due_usd else "PENDIENTE"
    else:
        factura.estado_cobranza = "PAGADA" if total_paid_cs >= due_cs else "PENDIENTE"

    db.commit()
    return JSONResponse({"ok": True, "message": "Abono registrado"})


@router.post("/sales/cobranza/{venta_id}/abono/{abono_id}")
async def sales_cobranza_abono_update(
    venta_id: int,
    abono_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    form = await request.form()
    moneda = (form.get("moneda") or "CS").upper()
    monto_raw = form.get("monto")
    observacion = (form.get("observacion") or "").strip()
    if not monto_raw:
        return JSONResponse({"ok": False, "message": "Monto requerido"}, status_code=400)
    if moneda not in {"CS", "USD"}:
        return JSONResponse({"ok": False, "message": "Moneda invalida"}, status_code=400)

    def parse_decimal(value: str) -> Decimal:
        raw = re.sub(r"[^0-9.,-]", "", str(value or "0"))
        if "," in raw and "." in raw:
            if raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "," in raw and "." not in raw:
            parts = raw.split(",")
            if len(parts) == 2 and len(parts[1]) == 2:
                raw = raw.replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "." in raw and "," not in raw:
            parts = raw.split(".")
            if len(parts) == 2 and len(parts[1]) == 2:
                raw = raw
            else:
                raw = raw.replace(".", "")
        try:
            return Decimal(raw)
        except Exception:
            return Decimal("0")

    monto = parse_decimal(monto_raw)
    if monto <= 0:
        return JSONResponse({"ok": False, "message": "Monto invalido"}, status_code=400)

    factura = db.query(VentaFactura).filter(VentaFactura.id == venta_id).first()
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and factura.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Factura fuera de tu bodega"}, status_code=403)

    abono = db.query(CobranzaAbono).filter(CobranzaAbono.id == abono_id, CobranzaAbono.factura_id == factura.id).first()
    if not abono:
        return JSONResponse({"ok": False, "message": "Abono no encontrado"}, status_code=404)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if moneda == "USD" and not rate_today:
        return JSONResponse({"ok": False, "message": "Tasa no configurada"}, status_code=400)
    tasa = Decimal(str(rate_today.rate)) if rate_today else Decimal("0")

    if moneda == "USD":
        monto_usd = monto
        monto_cs = monto * tasa
    else:
        monto_cs = monto
        monto_usd = monto / tasa if tasa else Decimal("0")

    abono.moneda = moneda
    abono.tasa_cambio = tasa if tasa else None
    abono.monto_usd = monto_usd
    abono.monto_cs = monto_cs
    abono.observacion = observacion
    db.commit()

    total_abono_usd = sum(Decimal(str(a.monto_usd or 0)) for a in factura.abonos)
    total_abono_cs = sum(Decimal(str(a.monto_cs or 0)) for a in factura.abonos)
    due_usd = Decimal(str(factura.total_usd or 0))
    due_cs = Decimal(str(factura.total_cs or 0))
    if (factura.moneda or "CS") == "USD":
        factura.estado_cobranza = "PAGADA" if total_abono_usd >= due_usd else "PENDIENTE"
    else:
        factura.estado_cobranza = "PAGADA" if total_abono_cs >= due_cs else "PENDIENTE"
    db.commit()

    return JSONResponse({"ok": True, "message": "Abono actualizado"})


@router.post("/sales/cobranza/{venta_id}/abono/{abono_id}/delete")
def sales_cobranza_abono_delete(
    venta_id: int,
    abono_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    factura = db.query(VentaFactura).filter(VentaFactura.id == venta_id).first()
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    if factura.estado == "ANULADA":
        return JSONResponse({"ok": False, "message": "Factura anulada"}, status_code=400)
    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and factura.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Factura fuera de tu bodega"}, status_code=403)

    abono = (
        db.query(CobranzaAbono)
        .filter(CobranzaAbono.id == abono_id, CobranzaAbono.factura_id == factura.id)
        .first()
    )
    if not abono:
        return JSONResponse({"ok": False, "message": "Abono no encontrado"}, status_code=404)

    db.delete(abono)

    total_abono_usd = sum(Decimal(str(a.monto_usd or 0)) for a in factura.abonos if a.id != abono_id)
    total_abono_cs = sum(Decimal(str(a.monto_cs or 0)) for a in factura.abonos if a.id != abono_id)
    due_usd = Decimal(str(factura.total_usd or 0))
    due_cs = Decimal(str(factura.total_cs or 0))
    if (factura.moneda or "CS") == "USD":
        factura.estado_cobranza = "PAGADA" if total_abono_usd >= due_usd else "PENDIENTE"
    else:
        factura.estado_cobranza = "PAGADA" if total_abono_cs >= due_cs else "PENDIENTE"
    db.commit()

    return JSONResponse({"ok": True, "message": "Abono eliminado"})


@router.post("/sales/cobranza/{venta_id}/estado")
async def sales_cobranza_estado(
    venta_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_user_web),
):
    form = await request.form()
    estado = (form.get("estado") or "PENDIENTE").upper()
    if estado not in {"PENDIENTE", "PAGADA"}:
        return JSONResponse({"ok": False, "message": "Estado invalido"}, status_code=400)
    factura = db.query(VentaFactura).filter(VentaFactura.id == venta_id).first()
    if not factura:
        return JSONResponse({"ok": False, "message": "Factura no encontrada"}, status_code=404)
    _, bodega = _resolve_branch_bodega(db, user)
    if bodega and factura.bodega_id != bodega.id:
        return JSONResponse({"ok": False, "message": "Factura fuera de tu bodega"}, status_code=403)
    factura.estado_cobranza = estado
    db.commit()
    return JSONResponse({"ok": True, "message": "Estado actualizado"})


@router.post("/inventory/import")
def inventory_import_products(
    file: UploadFile = File(...),
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error=Archivo+Excel+(.xlsx)+requerido", status_code=303)

    rate_today = (
        db.query(ExchangeRate)
        .filter(ExchangeRate.effective_date <= local_today())
        .order_by(ExchangeRate.effective_date.desc())
        .first()
    )
    if not rate_today:
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error=Tasa+de+cambio+no+configurada", status_code=303)

    content = file.file.read()
    if not content:
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error=Archivo+vacio", status_code=303)

    def to_decimal(value) -> Decimal:
        if value is None:
            return Decimal("0")
        if isinstance(value, (int, float, Decimal)):
            return Decimal(str(value))
        cleaned = str(value).strip().replace(",", "")
        if not cleaned:
            return Decimal("0")
        try:
            return Decimal(cleaned)
        except Exception:
            return Decimal("0")

    tasa = Decimal(str(rate_today.rate))
    wb = load_workbook(io.BytesIO(content), data_only=True)
    ws = wb.active
    header_row = [str(cell.value).strip() if cell.value is not None else "" for cell in ws[1]]
    header_map = {name.lower(): idx for idx, name in enumerate(header_row)}

    def col_idx(*names: str) -> Optional[int]:
        for name in names:
            idx = header_map.get(name.lower())
            if idx is not None:
                return idx
        return None

    idx_codigo = col_idx("codigo", "cod_producto", "cod")
    idx_desc = col_idx("descripcion", "descripción", "nombre")
    idx_linea = col_idx("linea")
    idx_segmento = col_idx("segmento")
    idx_costo_usd = col_idx("costo_usd", "costo_producto_usd", "costo")
    idx_precio_usd = col_idx("precio_usd", "precio_venta1_usd")
    idx_precio_cs = col_idx("precio_cs", "precio_cordobas", "precio_venta1")
    idx_saldo_central = col_idx("saldo_central", "existencia_central")
    idx_saldo_esteli = col_idx("saldo_esteli", "existencia_esteli")

    if idx_codigo is None or idx_desc is None:
        target = redirect_to or "/inventory"
        return RedirectResponse(f"{target}?error=Columnas+requeridas:+codigo+y+descripcion", status_code=303)

    bodegas = db.query(Bodega).filter(Bodega.activo.is_(True)).all()
    bodega_central = next(
        (b for b in bodegas if (b.code or "").lower() == "central"),
        None,
    )
    if not bodega_central:
        bodega_central = next((b for b in bodegas if "central" in (b.name or "").lower()), None)
    bodega_esteli = next(
        (b for b in bodegas if (b.code or "").lower() == "esteli"),
        None,
    )
    if not bodega_esteli:
        bodega_esteli = next((b for b in bodegas if "esteli" in (b.name or "").lower()), None)

    ingreso_tipo = (
        db.query(IngresoTipo)
        .filter(func.lower(IngresoTipo.nombre).like("%ajuste%"))
        .first()
    )
    if not ingreso_tipo:
        ingreso_tipo = (
            db.query(IngresoTipo)
            .filter(func.lower(IngresoTipo.nombre).like("%apertura%"))
            .first()
        )
    if not ingreso_tipo:
        ingreso_tipo = db.query(IngresoTipo).order_by(IngresoTipo.id).first()

    central_items: list[tuple[Producto, Decimal]] = []
    esteli_items: list[tuple[Producto, Decimal]] = []
    total_rows = 0
    skipped_rows = 0
    created_count = 0
    updated_count = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        total_rows += 1
        cod = str(row[idx_codigo]).strip() if row[idx_codigo] is not None else ""
        descripcion = str(row[idx_desc]).strip() if row[idx_desc] is not None else ""
        if not cod or not descripcion:
            skipped_rows += 1
            continue

        linea = None
        segmento = None
        linea_name = str(row[idx_linea]).strip() if idx_linea is not None and row[idx_linea] is not None else ""
        segmento_name = str(row[idx_segmento]).strip() if idx_segmento is not None and row[idx_segmento] is not None else ""
        if linea_name:
            linea = (
                db.query(Linea)
                .filter(func.lower(Linea.linea) == linea_name.lower())
                .first()
            )
            if not linea:
                linea = Linea(cod_linea=linea_name[:50], linea=linea_name, activo=True)
                db.add(linea)
                db.flush()
        if segmento_name:
            segmento = (
                db.query(Segmento)
                .filter(func.lower(Segmento.segmento) == segmento_name.lower())
                .first()
            )
            if not segmento:
                segmento = Segmento(segmento=segmento_name)
                db.add(segmento)
                db.flush()

        costo_usd = to_decimal(row[idx_costo_usd]) if idx_costo_usd is not None else Decimal("0")
        precio_usd = to_decimal(row[idx_precio_usd]) if idx_precio_usd is not None else Decimal("0")
        precio_cs = to_decimal(row[idx_precio_cs]) if idx_precio_cs is not None else Decimal("0")
        if precio_usd == 0 and precio_cs > 0:
            precio_usd = (precio_cs / tasa) if tasa else Decimal("0")
        if precio_cs == 0 and precio_usd > 0:
            precio_cs = precio_usd * tasa

        producto = db.query(Producto).filter(Producto.cod_producto == cod).first()
        if not producto:
            producto = Producto(
                cod_producto=cod,
                descripcion=descripcion,
                linea_id=linea.id if linea else None,
                segmento_id=segmento.id if segmento else None,
                precio_venta1=precio_cs,
                precio_venta2=Decimal("0"),
                precio_venta3=Decimal("0"),
                precio_venta1_usd=precio_usd,
                precio_venta2_usd=Decimal("0"),
                precio_venta3_usd=Decimal("0"),
                tasa_cambio=tasa,
                costo_producto=costo_usd * tasa,
                activo=True,
            )
            db.add(producto)
            db.flush()
            created_count += 1
        else:
            producto.descripcion = descripcion
            if linea:
                producto.linea_id = linea.id
            if segmento:
                producto.segmento_id = segmento.id
            if precio_usd > 0:
                producto.precio_venta1_usd = precio_usd
                producto.precio_venta1 = precio_cs
            if costo_usd > 0:
                producto.costo_producto = costo_usd * tasa
            producto.tasa_cambio = tasa
            updated_count += 1

        saldo_central = to_decimal(row[idx_saldo_central]) if idx_saldo_central is not None else Decimal("0")
        saldo_esteli = to_decimal(row[idx_saldo_esteli]) if idx_saldo_esteli is not None else Decimal("0")

        if bodega_central and saldo_central > 0:
            central_items.append((producto, saldo_central))
        if bodega_esteli and saldo_esteli > 0:
            esteli_items.append((producto, saldo_esteli))

        # No mezclar bodegas en el saldo global: se mantiene en cero y
        # el saldo por bodega se calcula desde los movimientos.
        saldo_row = db.query(SaldoProducto).filter(SaldoProducto.producto_id == producto.id).first()
        if not saldo_row:
            db.add(SaldoProducto(producto_id=producto.id, existencia=Decimal("0")))
        else:
            saldo_row.existencia = Decimal("0")

    def create_ingreso(bodega: Bodega, items: list[tuple[Producto, Decimal]]) -> None:
        if not items:
            return
        ingreso = IngresoInventario(
            tipo_id=ingreso_tipo.id if ingreso_tipo else 1,
            bodega_id=bodega.id,
            proveedor_id=None,
            fecha=local_today(),
            moneda="USD",
            tasa_cambio=tasa,
            total_usd=Decimal("0"),
            total_cs=Decimal("0"),
            observacion="Carga inicial por importacion",
            usuario_registro="Importador",
        )
        db.add(ingreso)
        db.flush()
        total_usd = Decimal("0")
        total_cs = Decimal("0")
        for producto, qty in items:
            costo_unit_usd = Decimal(str(producto.costo_producto or 0)) / tasa if tasa else Decimal("0")
            subtotal_usd = costo_unit_usd * qty
            subtotal_cs = subtotal_usd * tasa
            total_usd += subtotal_usd
            total_cs += subtotal_cs
            db.add(
                IngresoItem(
                    ingreso_id=ingreso.id,
                    producto_id=producto.id,
                    cantidad=qty,
                    costo_unitario_usd=costo_unit_usd,
                    costo_unitario_cs=costo_unit_usd * tasa,
                    subtotal_usd=subtotal_usd,
                    subtotal_cs=subtotal_cs,
                )
            )
        ingreso.total_usd = total_usd
        ingreso.total_cs = total_cs

    if bodega_central:
        create_ingreso(bodega_central, central_items)
    if bodega_esteli:
        create_ingreso(bodega_esteli, esteli_items)

    db.commit()
    target = redirect_to or "/inventory"
    msg = (
        f"Importacion completa. Filas: {total_rows}. "
        f"Creados: {created_count}. Actualizados: {updated_count}. Omitidos: {skipped_rows}."
    )
    return RedirectResponse(f"{target}?success={msg}", status_code=303)


@router.get("/inventory/import/template")
def inventory_import_template(
    _: User = Depends(_require_admin_web),
):
    headers = [
        "codigo",
        "descripcion",
        "linea",
        "segmento",
        "costo_usd",
        "precio_usd",
        "precio_cs",
        "saldo_central",
        "saldo_esteli",
    ]
    wb = Workbook()
    ws = wb.active
    ws.title = "Inventario"
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")
    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=productos_template.xlsx"},
    )


@router.post("/inventory/import/preview")
def inventory_import_preview(
    file: UploadFile = File(...),
    _: User = Depends(_require_admin_web),
):
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        return HTMLResponse(
            "<div class='alert alert-warning py-2 px-3'>Archivo Excel (.xlsx) requerido.</div>"
        )
    content = file.file.read()
    if not content:
        return HTMLResponse(
            "<div class='alert alert-warning py-2 px-3'>Archivo vacio.</div>"
        )
    wb = load_workbook(io.BytesIO(content), data_only=True)
    ws = wb.active
    headers = [str(cell.value).strip() if cell.value is not None else "" for cell in ws[1]]
    rows = []
    total_rows = 0
    skipped_rows = 0
    idx_codigo = None
    idx_desc = None
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row:
            continue
        total_rows += 1
        rows.append([cell if cell is not None else "" for cell in row])
    if headers:
        header_map = {name.lower(): idx for idx, name in enumerate(headers)}
        idx_codigo = header_map.get("codigo")
        idx_desc = header_map.get("descripcion")
    if idx_codigo is not None or idx_desc is not None:
        for row in rows:
            cod = str(row[idx_codigo]).strip() if idx_codigo is not None and row[idx_codigo] is not None else ""
            desc = str(row[idx_desc]).strip() if idx_desc is not None and row[idx_desc] is not None else ""
            if not cod or not desc:
                skipped_rows += 1
    if not headers:
        return HTMLResponse(
            "<div class='alert alert-warning py-2 px-3'>No se detectaron columnas.</div>"
        )
    table = [
        "<div class='mt-3'>",
        "<div class='fw-semibold mb-2'>Vista previa (todas las filas)</div>",
        f"<div class='small text-muted mb-2'>Filas detectadas: {total_rows}. Omitidas por codigo/descripcion vacios: {skipped_rows}.</div>",
        "<div class='table-responsive'>",
        "<table class='table table-sm table-striped align-middle'>",
        "<thead><tr>",
    ]
    for h in headers:
        table.append(f"<th class='text-nowrap'>{h}</th>")
    table.append("</tr></thead><tbody>")
    for row in rows:
        table.append("<tr>")
        for cell in row:
            table.append(f"<td class='text-nowrap'>{cell}</td>")
        table.append("</tr>")
    table.append("</tbody></table></div></div>")
    return HTMLResponse("".join(table))


@router.post("/inventory/import/reset")
def inventory_import_reset(
    redirect_to: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    # Guard rail: avoid wiping products if there are sales linked.
    ventas_count = db.query(VentaItem).count()
    if ventas_count > 0:
        target = redirect_to or "/inventory"
        return RedirectResponse(
            f"{target}?error=No+se+puede+limpiar+inventario+con+ventas+registradas",
            status_code=303,
        )

    db.query(IngresoItem).delete()
    db.query(IngresoInventario).delete()
    db.query(EgresoItem).delete()
    db.query(EgresoInventario).delete()
    db.query(SaldoProducto).delete()
    db.query(ProductoCombo).delete()
    db.query(Producto).delete()
    db.commit()

    target = redirect_to or "/inventory"
    return RedirectResponse(f"{target}?success=Inventario+limpio", status_code=303)


@router.get("/finance/rates")
def finance_rates(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(_require_admin_web),
):
    rates = db.query(ExchangeRate).order_by(ExchangeRate.effective_date.desc()).all()
    return request.app.state.templates.TemplateResponse(
        "finance_rates.html",
        {
            "request": request,
            "user": user,
            "rates": rates,
            "version": settings.UI_VERSION,
        },
    )


@router.post("/finance/rates")
def finance_rates_create(
    effective_date: date = Form(...),
    period: str = Form(...),
    rate: float = Form(...),
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    exists = (
        db.query(ExchangeRate)
        .filter(
            ExchangeRate.effective_date == effective_date,
            ExchangeRate.period == period,
        )
        .first()
    )
    if not exists:
        db.add(ExchangeRate(effective_date=effective_date, period=period, rate=rate))
        db.commit()
    return RedirectResponse("/finance/rates", status_code=303)


@router.post("/inventory/product/{product_id}/deactivate")
def inventory_deactivate_product(
    product_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if producto:
        existencia = float(producto.saldo.existencia) if producto.saldo else 0
        if existencia <= 0:
            producto.activo = False
            db.commit()
            return RedirectResponse("/inventory", status_code=303)
        return RedirectResponse("/inventory?error=No+se+puede+desactivar+con+existencia", status_code=303)
    return RedirectResponse("/inventory?error=Producto+no+encontrado", status_code=303)


@router.post("/inventory/product/{product_id}/activate")
def inventory_activate_product(
    product_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(_require_admin_web),
):
    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if producto:
        producto.activo = True
        db.commit()
        return RedirectResponse("/inventory", status_code=303)
    return RedirectResponse("/inventory?error=Producto+no+encontrado", status_code=303)

