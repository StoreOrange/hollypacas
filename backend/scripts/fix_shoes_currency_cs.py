#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import func

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.database import get_session_local
from app.models.inventory import IngresoInventario, IngresoItem, Producto


def d(value: object, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value if value is not None else default))
    except Exception:
        return Decimal(default)


def q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def run(rate: Decimal, dry_run: bool) -> None:
    session_local = get_session_local()
    db = session_local()
    try:
        ingresos = (
            db.query(IngresoInventario)
            .filter(
                IngresoInventario.moneda == "USD",
                func.lower(func.coalesce(IngresoInventario.observacion, "")).like("ingreso zapatos%"),
            )
            .order_by(IngresoInventario.id.asc())
            .all()
        )
        if not ingresos:
            print("No hay ingresos de zapatos en USD para corregir.")
            return

        ingreso_ids = [int(i.id) for i in ingresos]
        items = (
            db.query(IngresoItem)
            .filter(IngresoItem.ingreso_id.in_(ingreso_ids))
            .order_by(IngresoItem.ingreso_id.asc(), IngresoItem.id.asc())
            .all()
        )
        items_by_ingreso: dict[int, list[IngresoItem]] = {}
        product_ids: set[int] = set()
        for item in items:
            items_by_ingreso.setdefault(int(item.ingreso_id), []).append(item)
            product_ids.add(int(item.producto_id))

        fixed_items = 0
        fixed_ingresos = 0
        fixed_products = 0

        for ingreso in ingresos:
            rows = items_by_ingreso.get(int(ingreso.id), [])
            total_cs = Decimal("0")
            for row in rows:
                # Correccion principal:
                # lo que se guardo en USD era realmente C$ capturado por el usuario.
                new_cost_cs = d(row.costo_unitario_usd)
                new_subtotal_cs = d(row.subtotal_usd)
                new_cost_usd = q4(new_cost_cs / rate) if rate > 0 else Decimal("0")
                new_subtotal_usd = q2(new_subtotal_cs / rate) if rate > 0 else Decimal("0")

                row.costo_unitario_cs = float(new_cost_cs)
                row.subtotal_cs = float(new_subtotal_cs)
                row.costo_unitario_usd = float(new_cost_usd)
                row.subtotal_usd = float(new_subtotal_usd)
                total_cs += new_subtotal_cs
                fixed_items += 1

            ingreso.moneda = "CS"
            ingreso.tasa_cambio = None
            ingreso.total_cs = float(q2(total_cs))
            ingreso.total_usd = float(q2(total_cs / rate)) if rate > 0 else 0.0
            fixed_ingresos += 1

        productos = (
            db.query(Producto)
            .filter(Producto.id.in_(sorted(product_ids)))
            .order_by(Producto.id.asc())
            .all()
        )
        for producto in productos:
            cost_cs = d(producto.costo_producto)
            p1_cs = d(producto.precio_venta1)
            p2_cs = d(producto.precio_venta2)
            p3_cs = d(producto.precio_venta3)
            p4_cs = d(getattr(producto, "precio_venta4", 0))
            p5_cs = d(getattr(producto, "precio_venta5", 0))
            p6_cs = d(getattr(producto, "precio_venta6", 0))
            p7_cs = d(getattr(producto, "precio_venta7", 0))

            if rate > 0:
                cost_cs = q2(cost_cs / rate)
                p1_cs = q2(p1_cs / rate)
                p2_cs = q2(p2_cs / rate)
                p3_cs = q2(p3_cs / rate)
                p4_cs = q2(p4_cs / rate)
                p5_cs = q2(p5_cs / rate)
                p6_cs = q2(p6_cs / rate)
                p7_cs = q2(p7_cs / rate)

            producto.costo_producto = float(cost_cs)
            producto.precio_venta1 = float(p1_cs)
            producto.precio_venta2 = float(p2_cs)
            producto.precio_venta3 = float(p3_cs)
            producto.precio_venta4 = float(p4_cs)
            producto.precio_venta5 = float(p5_cs)
            producto.precio_venta6 = float(p6_cs)
            producto.precio_venta7 = float(p7_cs)
            producto.precio_venta1_usd = float(q4(p1_cs / rate)) if rate > 0 else 0.0
            producto.precio_venta2_usd = float(q4(p2_cs / rate)) if rate > 0 else 0.0
            producto.precio_venta3_usd = float(q4(p3_cs / rate)) if rate > 0 else 0.0
            producto.precio_venta4_usd = float(q4(p4_cs / rate)) if rate > 0 else 0.0
            producto.precio_venta5_usd = float(q4(p5_cs / rate)) if rate > 0 else 0.0
            producto.precio_venta6_usd = float(q4(p6_cs / rate)) if rate > 0 else 0.0
            producto.precio_venta7_usd = float(q4(p7_cs / rate)) if rate > 0 else 0.0
            fixed_products += 1

        if dry_run:
            db.rollback()
            print("[DRY RUN] Cambios calculados pero no aplicados.")
        else:
            db.commit()
            print("Correccion aplicada.")
        print(f"Ingresos corregidos: {fixed_ingresos}")
        print(f"Items corregidos: {fixed_items}")
        print(f"Productos ajustados: {fixed_products}")
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corrige ingresos de zapatos capturados en C$ pero guardados como USD.",
    )
    parser.add_argument("--rate", type=Decimal, default=Decimal("37"), help="Tasa para desconversion (default: 37)")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar conteos, sin guardar cambios")
    args = parser.parse_args()
    run(rate=args.rate, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
