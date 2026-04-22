from datetime import date
from decimal import Decimal

from sqlalchemy import func, inspect, text
from sqlalchemy.orm import Session

from ..config import get_active_company_key, settings
from ..database import Base, get_engine, get_session_local
from ..models.user import Branch, Permission, Role, User
from ..models.inventory import (
    Bodega,
    EgresoTipo,
    IngresoInventario,
    IngresoItem,
    IngresoTipo,
    Linea,
    Marca,
    Producto,
    ProductoReceta,
    ProductoRecetaLinea,
    SaldoProducto,
    Segmento,
    UnidadMedida,
)
from ..models.sales import (
    AccountingPolicySetting,
    AccountingVoucherType,
    Banco,
    CompanyProfileSetting,
    CuentaContable,
    CuentaBancaria,
    EmailConfig,
    ReciboMotivo,
    ReciboRubro,
    FormaPago,
    NotificationRecipient,
    PosPrintSetting,
    RestaurantTable,
    MobilePushSubscription,
    SalesInterfaceSetting,
    Vendedor,
)
from .security import hash_password


def _seed_roles(db: Session) -> None:
    role_names = ["administrador", "vendedor", "cajero", "seguridad", "contador", "bodega"]
    existing = {role.name for role in db.query(Role).all()}
    for name in role_names:
        if name not in existing:
            db.add(Role(name=name))
    db.commit()


def _seed_admin(db: Session) -> None:
    admin = db.query(User).filter(User.email == settings.ADMIN_EMAIL).first()
    admin_role = db.query(Role).filter(Role.name == "administrador").first()
    if admin:
        if admin_role and admin_role not in admin.roles:
            admin.roles.append(admin_role)
            db.commit()
        return

    admin = User(
        full_name=settings.ADMIN_FULL_NAME,
        email=settings.ADMIN_EMAIL,
        hashed_password=hash_password(settings.ADMIN_PASSWORD),
        is_active=True,
    )
    if admin_role:
        admin.roles.append(admin_role)
    db.add(admin)
    db.commit()


def _seed_permissions(db: Session) -> None:
    permission_names = [
        "Ecommerce",
        "Finanzas",
        "Inventarios",
        "Contabilidad",
        "Gestion de TI",
        "menu.home",
        "menu.sales",
        "menu.sales.caliente",
        "menu.sales.utilitario",
        "menu.sales.cobranza",
        "menu.sales.roc",
        "menu.sales.depositos",
        "menu.sales.cierre",
        "menu.sales.comisiones",
        "menu.sales.preventas",
        "menu.sales.preventas.mobile",
        "menu.inventory",
        "menu.inventory.caliente",
        "menu.inventory.ingresos",
        "menu.inventory.egresos",
        "menu.inventory.requisas",
        "menu.finance",
        "menu.accounting",
        "menu.reports",
        "menu.data",
        "access.sales",
        "access.sales.caliente",
        "access.sales.registrar",
        "access.sales.pagos",
        "access.sales.utilitario",
        "access.sales.cobranza",
        "access.sales.roc",
        "access.sales.depositos",
        "access.sales.cierre",
        "access.sales.reversion",
        "access.sales.comisiones",
        "access.sales.preventas",
        "access.sales.preventas.mobile",
        "access.inventory",
        "access.inventory.caliente",
        "access.inventory.ingresos",
        "access.inventory.egresos",
        "access.inventory.requisas",
        "access.inventory.productos",
        "access.finance",
        "access.finance.rates",
        "access.accounting",
        "access.accounting.financial_data",
        "access.accounting.entries",
        "access.accounting.voucher_types",
        "access.reports",
        "access.data",
        "access.data.permissions",
        "access.data.users",
        "access.data.roles",
        "access.data.catalogs",
    ]
    existing = {perm.name for perm in db.query(Permission).all()}
    for name in permission_names:
        if name not in existing:
            db.add(Permission(name=name))
    db.commit()


def _seed_branches(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    multi_branch_enabled = active_company not in {"comestibles", "barrera", "bdtrend"}
    if active_company == "bdzapatos":
        branches = [
            (
                "central",
                "Central",
                "BD Zapatos",
                "",
                "",
                "Sucursal Central",
            ),
            (
                "kg",
                "KG",
                "BD Zapatos",
                "",
                "",
                "Sucursal KG",
            ),
            (
                "kgf",
                "KGF",
                "BD Zapatos",
                "",
                "",
                "Sucursal KGF",
            ),
        ]
    elif active_company == "barrera":
        branches = [
            (
                "central",
                "La Barrera",
                "La Barrera Restaurante",
                "",
                "",
                "Sucursal principal",
            ),
        ]
    else:
        default_company_name = "Pacas Global" if active_company == "bdtrend" else "Hollywood Pacas"
        branches = [
            (
                "central",
                "Central",
                default_company_name,
                "0012202910068H",
                "8900-0300",
                "Managua, De los semaforos del colonial 10 vrs. al lago frente al pillin.",
            ),
        ]
        if multi_branch_enabled:
            branches.append(
                (
                    "esteli",
                    "Sucursal Esteli",
                    default_company_name,
                    "0012202910068H",
                    "8900-0300",
                    "Esteli, De auto lote del Norte 7 cuadras al este.",
                )
            )
    existing = {branch.code for branch in db.query(Branch).all()}
    for code, name, company_name, ruc, telefono, direccion in branches:
        if code not in existing:
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
        else:
            db.query(Branch).filter(Branch.code == code).update(
                {
                    "company_name": company_name,
                    "ruc": ruc,
                    "telefono": telefono,
                    "direccion": direccion,
                }
            )
        db.query(Branch).filter(Branch.code == code).update({"activo": True})
    if active_company == "bdzapatos":
        db.query(Branch).filter(Branch.code == "esteli").update({"activo": False})
    if active_company == "barrera":
        db.query(Branch).filter(Branch.code == "esteli").update({"activo": False})
    if not multi_branch_enabled:
        db.query(Branch).filter(Branch.code == "esteli").update({"activo": False})
    db.commit()


def _seed_role_permissions(db: Session) -> None:
    role_names = ["administrador", "seguridad"]
    permissions = db.query(Permission).all()
    for role_name in role_names:
        role = db.query(Role).filter(Role.name == role_name).first()
        if role and permissions:
            role.permissions = permissions
    contador = db.query(Role).filter(Role.name == "contador").first()
    if contador and not contador.permissions:
        contador_perm_names = {
            "menu.home",
            "menu.reports",
            "menu.accounting",
            "access.reports",
            "access.accounting",
            "access.accounting.financial_data",
            "access.accounting.entries",
            "access.accounting.voucher_types",
        }
        contador.permissions = (
            db.query(Permission).filter(Permission.name.in_(contador_perm_names)).all()
        )
    db.commit()


def _seed_accounting_voucher_types(db: Session) -> None:
    defaults = [
        ("DIARIO", "Comprobante diario", "CD"),
        ("INGRESO", "Comprobante de ingreso", "CI"),
        ("EGRESO", "Comprobante de egreso", "CE"),
        ("AJUSTE", "Comprobante de ajuste", "CA"),
    ]
    existing = {item.code.upper(): item for item in db.query(AccountingVoucherType).all()}
    changed = False
    for code, nombre, prefijo in defaults:
        row = existing.get(code)
        if not row:
            db.add(AccountingVoucherType(code=code, nombre=nombre, prefijo=prefijo, activo=True))
            changed = True
            continue
        if (row.nombre or "") != nombre:
            row.nombre = nombre
            changed = True
        if (row.prefijo or "") != prefijo:
            row.prefijo = prefijo
            changed = True
        if row.activo is None:
            row.activo = True
            changed = True
    if changed:
        db.commit()


def _seed_accounting_policy_settings(db: Session) -> None:
    row = db.query(AccountingPolicySetting).order_by(AccountingPolicySetting.id.asc()).first()
    if row:
        changed = False
        if row.strict_mode is None:
            row.strict_mode = True
            changed = True
        if row.auto_entry_enabled is None:
            row.auto_entry_enabled = False
            changed = True
        if not (row.ingreso_debe_terms or "").strip():
            row.ingreso_debe_terms = "caja,banco,cliente,cobrar"
            changed = True
        if not (row.ingreso_haber_terms or "").strip():
            row.ingreso_haber_terms = "venta,ingreso"
            changed = True
        if not (row.egreso_debe_terms or "").strip():
            row.egreso_debe_terms = "gasto,costo,compra,inventario"
            changed = True
        if not (row.egreso_haber_terms or "").strip():
            row.egreso_haber_terms = "caja,banco,proveedor,pagar"
            changed = True
        if changed:
            db.commit()
        return
    db.add(
        AccountingPolicySetting(
            strict_mode=True,
            auto_entry_enabled=False,
            ingreso_debe_terms="caja,banco,cliente,cobrar",
            ingreso_haber_terms="venta,ingreso",
            egreso_debe_terms="gasto,costo,compra,inventario",
            egreso_haber_terms="caja,banco,proveedor,pagar",
            updated_by="system-bootstrap",
        )
    )
    db.commit()


def _seed_admin_branch_access(db: Session) -> None:
    admin = db.query(User).filter(User.email == settings.ADMIN_EMAIL).first()
    if not admin:
        return
    active_branches = db.query(Branch).filter(Branch.activo.is_(True)).order_by(Branch.id).all()
    central_branch = db.query(Branch).filter(Branch.code == "central").first()
    active_ids = {branch.id for branch in active_branches}
    current_ids = {branch.id for branch in (admin.branches or [])}
    if active_ids - current_ids:
        admin.branches = list(admin.branches or []) + [branch for branch in active_branches if branch.id not in current_ids]
    if not admin.branches and central_branch:
        admin.branches = [central_branch]
    if central_branch and not admin.default_branch_id:
        admin.default_branch_id = central_branch.id
    if central_branch and not admin.default_bodega_id:
        central_bodega = (
            db.query(Bodega)
            .filter(Bodega.branch_id == central_branch.id, Bodega.activo.is_(True))
            .order_by(Bodega.id)
            .first()
        )
        if central_bodega:
            admin.default_bodega_id = central_bodega.id
    db.commit()


def _seed_lineas(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    if active_company == "comestibles":
        lineas = ["Consumibles"]
    elif active_company == "barrera":
        lineas = ["Cocina", "Barra", "Bebidas", "Postres", "Extras"]
    else:
        lineas = [
            "BLUSA",
            "BOLSOS",
            "BUSOS",
            "CALCETAS",
            "CAMISA",
            "CHAMARRA",
            "CHAQUETAS",
            "COLCHAS",
            "CONJIN",
            "CORTINAS",
            "EDREDON",
            "FALDA",
            "INTIMA",
            "JEAN",
            "JUGUETES",
            "MIX CLOTHING",
            "NINO",
            "PANTALON",
            "PELUCHES",
            "ROPA DE CAMA",
            "ROPA DE CASA",
            "SABANAS",
            "SHORT",
            "TOALLAS",
            "UNIFORME",
            "UTENCILIOS",
            "VESTIDO",
            "ZAPATOS/CALZADO",
        ]
    existing = {linea.linea for linea in db.query(Linea).all()}
    for name in lineas:
        if name not in existing:
            cod_linea = "CONSUMIBLES" if name == "Consumibles" else name
            db.add(Linea(cod_linea=cod_linea, linea=name, activo=True))
    db.commit()


def _seed_segmentos(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    if active_company == "comestibles":
        segmentos = [
            "Bebidas",
            "Varios",
            "Snacks",
            "Licores",
            "Comidas",
            "Perecederos",
            "Cosmeticos",
            "Dulces",
            "Hogar",
        ]
    elif active_company == "barrera":
        segmentos = [
            "Entradas",
            "Platos fuertes",
            "Bebidas",
            "Cocteles",
            "Postres",
            "Delivery",
            "Para llevar",
        ]
    else:
        segmentos = [
            "BOLSAS 25 LBS",
            "BOLSAS 50 LBS",
            "BOLSAS X LBS",
            "CAJA",
            "PACAS",
            "SACOS",
        ]
    existing = {segmento.segmento for segmento in db.query(Segmento).all()}
    for name in segmentos:
        if name not in existing:
            db.add(Segmento(segmento=name))
    db.commit()


def _seed_marcas(db: Session) -> None:
    existing = {m.nombre.lower() for m in db.query(Marca).all()}
    if "sin marca" not in existing:
        db.add(Marca(nombre="Sin Marca", activo=True))
        db.commit()


def _seed_bodegas(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    multi_branch_enabled = active_company not in {"comestibles", "barrera", "bdtrend"}
    branches = {branch.code: branch for branch in db.query(Branch).all()}
    if active_company == "bdzapatos":
        bodegas = [
            ("central", "Central", "central"),
            ("kg", "KG", "kg"),
            ("kgf", "KGF", "kgf"),
        ]
    elif active_company == "barrera":
        bodegas = [
            ("central", "La Barrera", "central"),
        ]
    else:
        bodegas = [
            ("central", "Central", "central"),
        ]
        if multi_branch_enabled:
            bodegas.append(("esteli", "Esteli", "esteli"))
    existing = {bodega.code for bodega in db.query(Bodega).all()}
    for code, name, branch_code in bodegas:
        if code not in existing and branch_code in branches:
            db.add(
                Bodega(
                    code=code,
                    name=name,
                    branch_id=branches[branch_code].id,
                    activo=True,
                )
            )
        elif code in existing and branch_code in branches:
            db.query(Bodega).filter(Bodega.code == code).update(
                {
                    "name": name,
                    "branch_id": branches[branch_code].id,
                    "activo": True,
                }
            )
    db.commit()
    if active_company == "bdzapatos":
        db.query(Bodega).filter(Bodega.code == "esteli").update({"activo": False})
        db.commit()
    if active_company == "barrera":
        db.query(Bodega).filter(Bodega.code == "esteli").update({"activo": False})
        db.commit()
    if not multi_branch_enabled:
        db.query(Bodega).filter(Bodega.code == "esteli").update({"activo": False})
        db.commit()


def _seed_ingreso_tipos(db: Session) -> None:
    tipos = [
        ("Compras Locales", True),
        ("Importacion", True),
        ("Ajustes de Inventario", False),
        ("Produccion", False),
        ("Apertura de Pacas", False),
        ("Clasificacion de mermas", False),
        ("Perdidas", False),
    ]
    existing = {tipo.nombre for tipo in db.query(IngresoTipo).all()}
    for nombre, requiere_proveedor in tipos:
        if nombre not in existing:
            db.add(IngresoTipo(nombre=nombre, requiere_proveedor=requiere_proveedor))
        else:
            db.query(IngresoTipo).filter(IngresoTipo.nombre == nombre).update(
                {"requiere_proveedor": requiere_proveedor}
            )
    db.commit()


def _seed_egreso_tipos(db: Session) -> None:
    tipos = [
        "Inventario Fisico",
        "Traslado entre bodegas",
        "Merma",
        "Perdida",
        "Reposicion a Cliente",
        "Produccion de Abierta",
        "Produccion embalaje",
        "Produccion perdida",
        "Ajuste por Faltante",
    ]
    existing = {tipo.nombre for tipo in db.query(EgresoTipo).all()}
    for nombre in tipos:
        if nombre not in existing:
            db.add(EgresoTipo(nombre=nombre))
    db.commit()


def _seed_formas_pago(db: Session) -> None:
    formas = ["Tarjeta", "Tarjeta Afiliacion", "Banco", "Efectivo", "Credito", "Anticipo"]
    existing = {forma.nombre for forma in db.query(FormaPago).all()}
    for nombre in formas:
        if nombre not in existing:
            db.add(FormaPago(nombre=nombre))
    db.commit()


def _seed_bancos(db: Session) -> None:
    bancos = ["BAC", "LAFISE", "BANPRO", "BDF", "FICHOSA", "AVANZ"]
    existing = {banco.nombre for banco in db.query(Banco).all()}
    for nombre in bancos:
        if nombre not in existing:
            db.add(Banco(nombre=nombre))
    db.commit()


def _seed_cuentas_bancarias(db: Session) -> None:
    bancos = db.query(Banco).all()
    existing = {(cuenta.banco_id, cuenta.moneda) for cuenta in db.query(CuentaBancaria).all()}
    for banco in bancos:
        for moneda in ["USD", "CS"]:
            key = (banco.id, moneda)
            if key not in existing:
                db.add(CuentaBancaria(banco_id=banco.id, moneda=moneda, cuenta=None))
    db.commit()


def _seed_vendedores(db: Session) -> None:
    nombres = ["Vendedor de Piso"]
    existing = {vendedor.nombre for vendedor in db.query(Vendedor).all()}
    for nombre in nombres:
        if nombre not in existing:
            db.add(Vendedor(nombre=nombre, activo=True))
    db.commit()


def _seed_restaurant_tables(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    if active_company != "barrera":
        return
    branch = db.query(Branch).filter(func.lower(Branch.code) == "central").first()
    bodega = db.query(Bodega).filter(func.lower(Bodega.code) == "central").first()
    if not branch or not bodega:
        return
    defaults = [
        ("M-01", "Mesa 1", "Salon principal", "ROUND", 4, 10, 10, 18, 1, 1),
        ("M-02", "Mesa 2", "Salon principal", "ROUND", 4, 20, 38, 18, 1, 1),
        ("M-03", "Mesa 3", "Salon principal", "ROUND", 4, 30, 66, 18, 1, 1),
    ]
    existing = {
        (int(item.branch_id), (item.code or "").strip().upper()): item
        for item in db.query(RestaurantTable).all()
    }
    changed = False
    default_codes = {code.upper() for code, *_ in defaults}
    for code, name, sector, shape, seats, sort_order, pos_x, pos_y, width_units, height_units in defaults:
        key = (int(branch.id), code.upper())
        row = existing.get(key)
        if not row:
            db.add(
                RestaurantTable(
                    branch_id=branch.id,
                    bodega_id=bodega.id,
                    code=code,
                    name=name,
                    sector=sector,
                    shape=shape,
                    seats=seats,
                    sort_order=sort_order,
                    pos_x=pos_x,
                    pos_y=pos_y,
                    width_units=width_units,
                    height_units=height_units,
                    active=True,
                )
            )
            changed = True
            continue
        if row.bodega_id != bodega.id:
            row.bodega_id = bodega.id
            changed = True
        if (row.name or "") != name:
            row.name = name
            changed = True
        if (row.sector or "") != sector:
            row.sector = sector
            changed = True
        if (row.shape or "") != shape:
            row.shape = shape
            changed = True
        if int(row.seats or 0) != int(seats):
            row.seats = seats
            changed = True
        if int(row.sort_order or 0) != int(sort_order):
            row.sort_order = sort_order
            changed = True
        if int(row.pos_x or 0) != int(pos_x):
            row.pos_x = pos_x
            changed = True
        if int(row.pos_y or 0) != int(pos_y):
            row.pos_y = pos_y
            changed = True
        if int(row.width_units or 0) != int(width_units):
            row.width_units = width_units
            changed = True
        if int(row.height_units or 0) != int(height_units):
            row.height_units = height_units
            changed = True
        if row.active is not True:
            row.active = True
            changed = True
    for row in db.query(RestaurantTable).filter(RestaurantTable.branch_id == branch.id).all():
        if (row.code or "").strip().upper() not in default_codes and row.active:
            row.active = False
            changed = True
    if changed:
        db.commit()


def _seed_restaurant_demo_products(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    if active_company != "barrera":
        return
    branch = db.query(Branch).filter(func.lower(Branch.code) == "central").first()
    bodega = db.query(Bodega).filter(func.lower(Bodega.code) == "central").first()
    ingreso_tipo = db.query(IngresoTipo).filter(IngresoTipo.nombre == "Ajustes de Inventario").first()
    if not branch or not bodega or not ingreso_tipo:
        return
    lineas = {row.linea: row for row in db.query(Linea).all()}
    segmentos = {row.segmento: row for row in db.query(Segmento).all()}
    defaults = [
        ("BAR-001", "Botella de Agua de 500 ml", "Bebidas", "Bebidas", Decimal("45.00"), Decimal("25.00"), Decimal("10.00")),
        ("BAR-002", "Coctel azul", "Barra", "Cocteles", Decimal("125.00"), Decimal("105.00"), Decimal("10.00")),
        ("BAR-003", "Enchiladitas", "Cocina", "Entradas", Decimal("60.00"), Decimal("40.00"), Decimal("10.00")),
        ("BAR-004", "Expresso de caramelo", "Barra", "Bebidas", Decimal("150.00"), Decimal("130.00"), Decimal("10.00")),
        ("BAR-005", "Capuchino", "Barra", "Bebidas", Decimal("100.00"), Decimal("80.00"), Decimal("10.00")),
    ]
    product_ids: list[int] = []
    changed = False
    for code, description, linea_name, segmento_name, price_cs, cost_cs, initial_qty in defaults:
        product = db.query(Producto).filter(func.lower(Producto.cod_producto) == code.lower()).first()
        if not product:
            product = Producto(
                cod_producto=code,
                descripcion=description,
                linea_id=lineas.get(linea_name).id if lineas.get(linea_name) else None,
                segmento_id=segmentos.get(segmento_name).id if segmentos.get(segmento_name) else None,
                precio_venta1=price_cs,
                costo_producto=cost_cs,
                servicio_producto=False,
                activo=True,
                usuario_registro="system-barrera-seed",
            )
            db.add(product)
            db.flush()
            db.add(SaldoProducto(producto_id=product.id, existencia=Decimal("0")))
            changed = True
        else:
            if (product.descripcion or "") != description:
                product.descripcion = description
                changed = True
            if int(product.linea_id or 0) != int(lineas.get(linea_name).id if lineas.get(linea_name) else 0):
                product.linea_id = lineas.get(linea_name).id if lineas.get(linea_name) else None
                changed = True
            if int(product.segmento_id or 0) != int(segmentos.get(segmento_name).id if segmentos.get(segmento_name) else 0):
                product.segmento_id = segmentos.get(segmento_name).id if segmentos.get(segmento_name) else None
                changed = True
            if str(product.precio_venta1 or 0) != str(price_cs):
                product.precio_venta1 = price_cs
                changed = True
            if str(product.costo_producto or 0) != str(cost_cs):
                product.costo_producto = cost_cs
                changed = True
            saldo = db.query(SaldoProducto).filter(SaldoProducto.producto_id == product.id).first()
            if not saldo:
                db.add(SaldoProducto(producto_id=product.id, existencia=Decimal("0")))
                changed = True
        product_ids.append(int(product.id))
    if changed:
        db.commit()

    marker = "SEED-LA-BARRERA-MENU-INITIAL"
    existing_ingreso = (
        db.query(IngresoInventario)
        .filter(IngresoInventario.bodega_id == bodega.id, IngresoInventario.observacion == marker)
        .first()
    )
    if existing_ingreso:
        return

    ingreso = IngresoInventario(
        tipo_id=ingreso_tipo.id,
        bodega_id=bodega.id,
        proveedor_id=None,
        fecha=date.today(),
        moneda="CS",
        tasa_cambio=None,
        total_usd=Decimal("0"),
        total_cs=Decimal("0"),
        observacion=marker,
        usuario_registro="system-barrera-seed",
    )
    db.add(ingreso)
    db.flush()
    total_cs = Decimal("0")
    for code, _, _, _, _, cost_cs, initial_qty in defaults:
        product = db.query(Producto).filter(func.lower(Producto.cod_producto) == code.lower()).first()
        if not product:
            continue
        subtotal_cs = (cost_cs * initial_qty).quantize(Decimal("0.01"))
        db.add(
            IngresoItem(
                ingreso_id=ingreso.id,
                producto_id=product.id,
                cantidad=initial_qty,
                costo_unitario_usd=Decimal("0"),
                costo_unitario_cs=cost_cs,
                subtotal_usd=Decimal("0"),
                subtotal_cs=subtotal_cs,
            )
        )
        total_cs += subtotal_cs
    ingreso.total_cs = total_cs.quantize(Decimal("0.01"))
    ingreso.total_usd = Decimal("0")
    db.commit()


def _seed_recibos_rubros(db: Session) -> None:
    rubros = [
        "Gastos Operativos",
        "Gastos administrativos",
        "Gastos financieros",
        "Gastos de Ventas",
        "Otros gastos",
        "Ingresos por venta",
        "Devoluciones sobre ventas",
        "Ventas netas",
        "Ingresos financieros",
        "Mantenimiento de valor",
        "Intereses bancarios",
        "Cuentas por cobrar clientes",
        "Cuentas por cobrar empleados",
        "Inventarios",
        "Vehiculos",
        "Depreciacion de vehiculos",
        "Depreciacion mob. y equipo de oficina",
        "Activo diferido",
        "Otros ingresos",
        "Costo de ventas",
        "Gastos de operacion",
        "Gastos de venta",
        "Gastos de administracion",
        "Intereses corrientes y moratorios",
        "Perdida por diferencia cambiaria",
        "Gastos y comisiones bancarias",
        "Otras cuentas por cobrar",
        "Impuestos por pagar",
        "Activo circulante",
        "Cajas y bancos",
        "Impuestos pagados por anticipado",
        "Activo fijo",
        "Mobiliario y equipo de oficina",
        "Gastos diferidos",
        "Seguros",
        "Matricula de alcaldia",
        "Pasivo circulante",
        "Proveedores nacionales",
        "Proveedores extranjeros",
        "Gastos acumulados por pagar",
        "Retenciones por pagar",
        "Cuentas por pagar corto plazo",
        "Prestamos por pagar vehiculos",
        "Pasivos fijos",
        "Cuentas por pagar largo plazo",
        "Capital social",
        "Aportacion de capital",
        "Utilidad/perdida acumulada periodo anterior",
        "Utilidad/perdida del ejercicio",
        "Ajuste periodos anteriores",
        "Reserva legal",
        "Provision cuentas incobrables",
        "Descuento sobre ventas",
        "Utilidad o perdida de operacion",
        "Utilidad o perdida del ejercicio",
        "Total pasivo",
        "A C T I V O",
        "Total activo",
        "Pasivos",
        "Capital",
        "Total pasivo mas capital",
    ]
    existing = {str(r.nombre or "").strip().lower() for r in db.query(ReciboRubro).all()}
    seen_in_batch: set[str] = set()
    for nombre in rubros:
        normalized = nombre.strip().lower()
        if not normalized:
            continue
        if normalized in existing or normalized in seen_in_batch:
            continue
        db.add(ReciboRubro(nombre=nombre.strip(), activo=True))
        seen_in_batch.add(normalized)
    db.commit()


def _seed_recibos_motivos(db: Session) -> None:
    ingresos = [
        "Ingreso por cambio",
        "Ingreso de efectivo",
        "Devolucion",
        "Recuperacion por cobranza",
        "Anticipo de cliente",
    ]
    egresos = [
        "Pago de Planilla 15nal",
        "Pago de servicios basicos",
        "Gastos de papeleria",
        "Gastos de ventas",
        "Compra de insumos",
        "Compra de materiales de limpieza",
        "Pago por servicios de seguridad",
    ]
    existing = {m.nombre for m in db.query(ReciboMotivo).all()}
    for nombre in ingresos:
        if nombre not in existing:
            db.add(ReciboMotivo(nombre=nombre, tipo="INGRESO", activo=True))
    for nombre in egresos:
        if nombre not in existing:
            db.add(ReciboMotivo(nombre=nombre, tipo="EGRESO", activo=True))
    db.commit()


def _seed_cuentas_contables(db: Session) -> None:
    cuentas = [
        # Nivel 1
        ("1", "Activo", "BALANCE", "DEBE", None),
        ("2", "Pasivo", "BALANCE", "HABER", None),
        ("3", "Patrimonio", "BALANCE", "HABER", None),
        ("4", "Ingresos", "RESULTADO", "HABER", None),
        ("5", "Costos", "RESULTADO", "DEBE", None),
        ("6", "Gastos", "RESULTADO", "DEBE", None),
        # Nivel 2 Activo
        ("11", "Activo Corriente", "BALANCE", "DEBE", "1"),
        ("12", "Activo No Corriente", "BALANCE", "DEBE", "1"),
        # Nivel 3 Activo Corriente
        ("1101", "Caja", "BALANCE", "DEBE", "11"),
        ("1102", "Bancos", "BALANCE", "DEBE", "11"),
        ("1103", "Cuentas por Cobrar", "BALANCE", "DEBE", "11"),
        ("1104", "Inventarios", "BALANCE", "DEBE", "11"),
        ("1105", "Anticipos a Proveedores", "BALANCE", "DEBE", "11"),
        ("1106", "Otros Activos Corrientes", "BALANCE", "DEBE", "11"),
        # Nivel 3 Activo No Corriente
        ("1201", "Propiedad, Planta y Equipo", "BALANCE", "DEBE", "12"),
        ("1202", "Depreciacion Acumulada", "BALANCE", "HABER", "12"),
        ("1203", "Activos Intangibles", "BALANCE", "DEBE", "12"),
        # Nivel 2 Pasivo
        ("21", "Pasivo Corriente", "BALANCE", "HABER", "2"),
        ("22", "Pasivo No Corriente", "BALANCE", "HABER", "2"),
        # Nivel 3 Pasivo Corriente
        ("2101", "Cuentas por Pagar", "BALANCE", "HABER", "21"),
        ("2102", "Proveedores", "BALANCE", "HABER", "21"),
        ("2103", "Obligaciones Fiscales", "BALANCE", "HABER", "21"),
        ("2104", "Obligaciones Laborales", "BALANCE", "HABER", "21"),
        ("2105", "Otros Pasivos Corrientes", "BALANCE", "HABER", "21"),
        # Nivel 3 Pasivo No Corriente
        ("2201", "Prestamos Bancarios", "BALANCE", "HABER", "22"),
        ("2202", "Otros Pasivos No Corrientes", "BALANCE", "HABER", "22"),
        # Nivel 2 Patrimonio
        ("31", "Capital", "BALANCE", "HABER", "3"),
        ("32", "Resultados Acumulados", "BALANCE", "HABER", "3"),
        # Nivel 3 Patrimonio
        ("3101", "Capital Social", "BALANCE", "HABER", "31"),
        ("3201", "Resultados del Ejercicio", "BALANCE", "HABER", "32"),
        # Nivel 2 Ingresos
        ("41", "Ingresos Operacionales", "RESULTADO", "HABER", "4"),
        ("42", "Otros Ingresos", "RESULTADO", "HABER", "4"),
        # Nivel 3 Ingresos
        ("4101", "Ventas", "RESULTADO", "HABER", "41"),
        ("4102", "Descuentos en Ventas", "RESULTADO", "DEBE", "41"),
        ("4201", "Otros Ingresos", "RESULTADO", "HABER", "42"),
        # Nivel 2 Costos
        ("51", "Costos de Venta", "RESULTADO", "DEBE", "5"),
        ("52", "Otros Costos", "RESULTADO", "DEBE", "5"),
        # Nivel 3 Costos
        ("5101", "Costo de Mercaderia Vendida", "RESULTADO", "DEBE", "51"),
        ("5201", "Otros Costos", "RESULTADO", "DEBE", "52"),
        # Nivel 2 Gastos
        ("61", "Gastos Operativos", "RESULTADO", "DEBE", "6"),
        ("62", "Gastos Administrativos", "RESULTADO", "DEBE", "6"),
        ("63", "Gastos Financieros", "RESULTADO", "DEBE", "6"),
        ("64", "Gastos de Ventas", "RESULTADO", "DEBE", "6"),
        ("69", "Otros Gastos", "RESULTADO", "DEBE", "6"),
        # Nivel 3 Gastos (referencia rubros)
        ("6101", "Gastos Operativos", "RESULTADO", "DEBE", "61"),
        ("6201", "Gastos Administrativos", "RESULTADO", "DEBE", "62"),
        ("6301", "Gastos Financieros", "RESULTADO", "DEBE", "63"),
        ("6401", "Gastos de Ventas", "RESULTADO", "DEBE", "64"),
        ("6901", "Otros Gastos", "RESULTADO", "DEBE", "69"),
        # Cuentas ampliadas de rubros solicitados
        ("1107", "Cuentas por Cobrar Clientes", "BALANCE", "DEBE", "11"),
        ("1108", "Cuentas por Cobrar Empleados", "BALANCE", "DEBE", "11"),
        ("1109", "Otras Cuentas por Cobrar", "BALANCE", "DEBE", "11"),
        ("1110", "Impuestos Pagados por Anticipado", "BALANCE", "DEBE", "11"),
        ("1111", "Provision Cuentas Incobrables", "BALANCE", "HABER", "11"),
        ("1204", "Vehiculos", "BALANCE", "DEBE", "12"),
        ("1205", "Mobiliario y Equipo de Oficina", "BALANCE", "DEBE", "12"),
        ("1206", "Depreciacion de Vehiculos", "BALANCE", "HABER", "12"),
        ("1207", "Depreciacion Mob. y Equipo de Oficina", "BALANCE", "HABER", "12"),
        ("1208", "Gastos Diferidos", "BALANCE", "DEBE", "12"),
        ("1209", "Seguros", "BALANCE", "DEBE", "12"),
        ("1210", "Matricula de Alcaldia", "BALANCE", "DEBE", "12"),
        ("1211", "Activo Diferido", "BALANCE", "DEBE", "12"),
        ("2106", "Impuestos por Pagar", "BALANCE", "HABER", "21"),
        ("2107", "Proveedores Nacionales", "BALANCE", "HABER", "21"),
        ("2108", "Proveedores Extranjeros", "BALANCE", "HABER", "21"),
        ("2109", "Gastos Acumulados por Pagar", "BALANCE", "HABER", "21"),
        ("2110", "Retenciones por Pagar", "BALANCE", "HABER", "21"),
        ("2111", "Cuentas por Pagar Corto Plazo", "BALANCE", "HABER", "21"),
        ("2203", "Prestamos por Pagar Vehiculos", "BALANCE", "HABER", "22"),
        ("2204", "Cuentas por Pagar Largo Plazo", "BALANCE", "HABER", "22"),
        ("3102", "Aportacion de Capital", "BALANCE", "HABER", "31"),
        ("3202", "Utilidad/Perdida Acum. Periodo Ant.", "BALANCE", "HABER", "32"),
        ("3203", "Utilidad/Perdida del Ejercicio", "BALANCE", "HABER", "32"),
        ("3204", "Ajuste Periodos Anteriores", "BALANCE", "HABER", "32"),
        ("3205", "Reserva Legal", "BALANCE", "HABER", "32"),
        ("4103", "Ingresos por Venta", "RESULTADO", "HABER", "41"),
        ("4104", "Devoluciones sobre Ventas", "RESULTADO", "DEBE", "41"),
        ("4105", "Ventas Netas", "RESULTADO", "HABER", "41"),
        ("4106", "Descuento sobre Ventas", "RESULTADO", "DEBE", "41"),
        ("4202", "Ingresos Financieros", "RESULTADO", "HABER", "42"),
        ("4203", "Mantenimiento de Valor", "RESULTADO", "HABER", "42"),
        ("4204", "Intereses Bancarios", "RESULTADO", "HABER", "42"),
        ("4205", "Otros Ingresos", "RESULTADO", "HABER", "42"),
        ("5102", "Costo de Ventas", "RESULTADO", "DEBE", "51"),
        ("6102", "Gastos de Operacion", "RESULTADO", "DEBE", "61"),
        ("6202", "Gastos de Administracion", "RESULTADO", "DEBE", "62"),
        ("6402", "Gastos de Venta", "RESULTADO", "DEBE", "64"),
        ("6302", "Intereses Corrientes y Moratorios", "RESULTADO", "DEBE", "63"),
        ("6303", "Perdida por Dif. Cambiarios", "RESULTADO", "DEBE", "63"),
        ("6304", "Gastos y Comisiones Bancarias", "RESULTADO", "DEBE", "63"),
    ]

    existing = {c.codigo: c for c in db.query(CuentaContable).all()}
    for codigo, nombre, tipo, naturaleza, parent_code in cuentas:
        if codigo not in existing:
            db.add(
                CuentaContable(
                    codigo=codigo,
                    nombre=nombre,
                    tipo=tipo,
                    naturaleza=naturaleza,
                    activo=True,
                )
            )
    db.commit()

    all_accounts = {c.codigo: c for c in db.query(CuentaContable).all()}
    for codigo, nombre, tipo, naturaleza, parent_code in cuentas:
        account = all_accounts.get(codigo)
        if not account:
            continue
        parent = all_accounts.get(parent_code) if parent_code else None
        account.parent_id = parent.id if parent else None
        account.nivel = (parent.nivel + 1) if parent else 1
    db.commit()


def _seed_pos_print_settings(db: Session) -> None:
    branches = db.query(Branch).all()
    existing = {setting.branch_id for setting in db.query(PosPrintSetting).all()}
    for branch in branches:
        if branch.id in existing:
            continue
        db.add(
            PosPrintSetting(
                branch_id=branch.id,
                printer_name="HP Receipt",
                copies=1,
                auto_print=True,
                cierre_copies=1,
            )
        )
    db.commit()


def _seed_email_config(db: Session) -> None:
    existing = db.query(EmailConfig).first()
    if not existing:
        db.add(EmailConfig(sender_email="orangetectec@zohomail.com", sender_name="Pacas Global"))
        db.commit()


def _seed_email_recipients(db: Session) -> None:
    recipients = ["oddgarcia.samuel@gmail.com"]
    existing = {r.email for r in db.query(NotificationRecipient).all()}
    for email in recipients:
        if email not in existing:
            db.add(NotificationRecipient(email=email, active=True, sales_close_active=False))
    db.commit()


def _seed_sales_interface_settings(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    if active_company in {"bdzapatos", "zapatos", "miss_zapatos"}:
        interface_default = "zapatos"
    elif active_company == "barrera":
        interface_default = "restaurante"
    else:
        interface_default = "ropa"
    existing = db.query(SalesInterfaceSetting).first()
    if not existing:
        db.add(SalesInterfaceSetting(interface_code=interface_default))
        db.commit()


def _seed_unidades_medida(db: Session) -> None:
    defaults = [
        ("UNIDAD", "Unidad", "und"),
        ("LIBRAS", "Libras", "lb"),
        ("KILOGRAMOS", "Kilogramos", "kg"),
        ("ONZAS", "Onzas", "oz"),
        ("MILILITROS", "Mililitros", "ml"),
    ]
    existing = {
        (row.codigo or "").strip().upper(): row
        for row in db.query(UnidadMedida).all()
    }
    changed = False
    for codigo, nombre, abreviatura in defaults:
        row = existing.get(codigo)
        if not row:
            db.add(
                UnidadMedida(
                    codigo=codigo,
                    nombre=nombre,
                    abreviatura=abreviatura,
                    activo=True,
                )
            )
            changed = True
            continue
        if row.nombre != nombre:
            row.nombre = nombre
            changed = True
        if (row.abreviatura or "") != abreviatura:
            row.abreviatura = abreviatura
            changed = True
        if row.activo is None:
            row.activo = True
            changed = True
    if changed:
        db.commit()


def _seed_company_profile_settings(db: Session) -> None:
    active_company = (get_active_company_key() or "").strip().lower()
    is_shoes = active_company in {"bdzapatos", "zapatos", "miss_zapatos"}
    is_restaurant = active_company == "barrera"
    is_comestibles = active_company == "comestibles"
    is_global = active_company == "bdtrend"
    multi_branch_enabled = active_company not in {"comestibles", "barrera", "bdtrend"}
    existing = db.query(CompanyProfileSetting).first()
    if existing:
        changed = False
        if is_shoes:
            if not (existing.legal_name or "").strip():
                existing.legal_name = "Miss Zapatos"
                changed = True
            if not (existing.trade_name or "").strip():
                existing.trade_name = "Miss Zapatos"
                changed = True
            if not (existing.app_title or "").strip():
                existing.app_title = "ERP Miss Zapatos"
                changed = True
            if not (existing.sidebar_subtitle or "").strip():
                existing.sidebar_subtitle = "ERP Zapateria"
                changed = True
        if is_restaurant:
            if not (existing.legal_name or "").strip() or existing.legal_name == "Hollywood Pacas":
                existing.legal_name = "La Barrera Restaurante"
                changed = True
            if not (existing.trade_name or "").strip() or existing.trade_name == "Hollywood Pacas":
                existing.trade_name = "La Barrera"
                changed = True
            if not (existing.app_title or "").strip() or existing.app_title == "ERP Hollywood Pacas":
                existing.app_title = "ERP La Barrera"
                changed = True
            if not (existing.sidebar_subtitle or "").strip() or existing.sidebar_subtitle == "ERP Central":
                existing.sidebar_subtitle = "Restaurante & Bar"
                changed = True
        if not (getattr(existing, "login_logo_url", "") or "").strip():
            existing.login_logo_url = (existing.pos_logo_url or "").strip() or (existing.logo_url or "").strip() or "/static/logo_hollywood.png"
            changed = True
        if (not is_shoes) and (not is_restaurant) and is_global:
            if not (existing.legal_name or "").strip() or existing.legal_name == "Hollywood Pacas":
                existing.legal_name = "Pacas Global"
                changed = True
            if not (existing.trade_name or "").strip() or existing.trade_name == "Hollywood Pacas":
                existing.trade_name = "Pacas Global"
                changed = True
            if not (existing.app_title or "").strip() or existing.app_title == "ERP Hollywood Pacas":
                existing.app_title = "ERP Pacas Global"
                changed = True
            if not (existing.email or "").strip() or existing.email == "admin@hollywoodpacas.com":
                existing.email = "admin@pacasglobal.com"
                changed = True
            if (existing.website or "").strip() == "http://hollywoodpacas.com.ni":
                existing.website = ""
                changed = True
        if (not is_shoes) and (not is_restaurant) and is_comestibles:
            if not (existing.legal_name or "").strip() or existing.legal_name in {"Hollywood Pacas", "Pacas Global"}:
                existing.legal_name = "Tienda de Conveniencia AMAJO"
                changed = True
            if not (existing.trade_name or "").strip() or existing.trade_name in {"Hollywood Pacas", "Pacas Global"}:
                existing.trade_name = "AMAJO"
                changed = True
            if not (existing.app_title or "").strip() or existing.app_title in {"ERP Hollywood Pacas", "ERP Pacas Global"}:
                existing.app_title = "ERP AMAJO"
                changed = True
            if not (existing.sidebar_subtitle or "").strip() or existing.sidebar_subtitle in {"ERP Central", "ERP Pacas Global"}:
                existing.sidebar_subtitle = "Tienda de Conveniencia"
                changed = True
        if (not is_shoes) and (not is_restaurant) and (not is_comestibles) and (not is_global):
            if not (existing.legal_name or "").strip() or existing.legal_name == "Pacas Global":
                existing.legal_name = "Hollywood Pacas"
                changed = True
            if not (existing.trade_name or "").strip() or existing.trade_name == "Pacas Global":
                existing.trade_name = "Hollywood Pacas"
                changed = True
            if not (existing.app_title or "").strip() or existing.app_title == "ERP Pacas Global":
                existing.app_title = "ERP Hollywood Pacas"
                changed = True
            if not (existing.sidebar_subtitle or "").strip():
                existing.sidebar_subtitle = "ERP Central"
                changed = True
            if not (existing.website or "").strip():
                existing.website = "http://hollywoodpacas.com.ni"
                changed = True
            if not (existing.email or "").strip() or existing.email == "admin@pacasglobal.com":
                existing.email = "admin@hollywoodpacas.com"
                changed = True
        if existing.multi_branch_enabled is None or bool(existing.multi_branch_enabled) != bool(multi_branch_enabled):
            existing.multi_branch_enabled = multi_branch_enabled
            changed = True
        if getattr(existing, "weighted_inventory_enabled", None) is None:
            existing.weighted_inventory_enabled = False
            changed = True
        if getattr(existing, "weighted_sales_enabled", None) is None:
            existing.weighted_sales_enabled = False
            changed = True
        if getattr(existing, "recipe_explosion_on_ingreso", None) is None:
            existing.recipe_explosion_on_ingreso = False
            changed = True
        if existing.price_auto_from_cost_enabled is None:
            existing.price_auto_from_cost_enabled = False
            changed = True
        if existing.price_margin_percent is None:
            existing.price_margin_percent = 0
            changed = True
        if not (getattr(existing, "theme_code", "") or "").strip():
            existing.theme_code = "default"
            changed = True
        if changed:
            db.commit()
        return
    if is_shoes:
        db.add(
            CompanyProfileSetting(
                legal_name="Miss Zapatos",
                trade_name="Miss Zapatos",
                app_title="ERP Miss Zapatos",
                sidebar_subtitle="ERP Zapateria",
                website="",
                ruc="",
                phone="",
                address="",
                email="",
                logo_url="/static/logo_hollywood.png",
                pos_logo_url="/static/logo_hollywood.png",
                login_logo_url="/static/logo_hollywood.png",
                favicon_url="/static/favicon.ico",
                inventory_cs_only=False,
                weighted_inventory_enabled=False,
                weighted_sales_enabled=False,
                recipe_explosion_on_ingreso=False,
                multi_branch_enabled=multi_branch_enabled,
                price_auto_from_cost_enabled=False,
                price_margin_percent=0,
                theme_code="default",
                updated_by="system-bootstrap",
            )
        )
    elif is_restaurant:
        db.add(
            CompanyProfileSetting(
                legal_name="La Barrera Restaurante",
                trade_name="La Barrera",
                app_title="ERP La Barrera",
                sidebar_subtitle="Restaurante & Bar",
                website="",
                ruc="",
                phone="",
                address="Sucursal principal",
                email="",
                logo_url="/static/logo_hollywood.png",
                pos_logo_url="/static/logo_hollywood.png",
                login_logo_url="/static/logo_hollywood.png",
                favicon_url="/static/favicon.ico",
                inventory_cs_only=False,
                weighted_inventory_enabled=False,
                weighted_sales_enabled=False,
                recipe_explosion_on_ingreso=False,
                multi_branch_enabled=multi_branch_enabled,
                price_auto_from_cost_enabled=False,
                price_margin_percent=0,
                theme_code="default",
                updated_by="system-bootstrap",
            )
        )
    elif is_comestibles:
        db.add(
            CompanyProfileSetting(
                legal_name="Tienda de Conveniencia AMAJO",
                trade_name="AMAJO",
                app_title="ERP AMAJO",
                sidebar_subtitle="Tienda de Conveniencia",
                website="",
                ruc="",
                phone="8900-0300",
                address="Sucursal principal",
                email="",
                logo_url="/static/logo_hollywood.png",
                pos_logo_url="/static/logo_hollywood.png",
                login_logo_url="/static/logo_hollywood.png",
                favicon_url="/static/favicon.ico",
                inventory_cs_only=False,
                weighted_inventory_enabled=False,
                weighted_sales_enabled=False,
                recipe_explosion_on_ingreso=False,
                multi_branch_enabled=multi_branch_enabled,
                price_auto_from_cost_enabled=False,
                price_margin_percent=0,
                theme_code="default",
                updated_by="system-bootstrap",
            )
        )
    elif is_global:
        db.add(
            CompanyProfileSetting(
                legal_name="Pacas Global",
                trade_name="Pacas Global",
                app_title="ERP Pacas Global",
                sidebar_subtitle="ERP Central",
                website="",
                ruc="",
                phone="8900-0300",
                address="Managua, De los semaforos del colonial 10 vrs. al lago frente al pillin.",
                email="admin@pacasglobal.com",
                logo_url="/static/logo_hollywood.png",
                pos_logo_url="/static/logo_hollywood.png",
                login_logo_url="/static/logo_hollywood.png",
                favicon_url="/static/favicon.ico",
                inventory_cs_only=False,
                weighted_inventory_enabled=False,
                weighted_sales_enabled=False,
                recipe_explosion_on_ingreso=False,
                multi_branch_enabled=multi_branch_enabled,
                price_auto_from_cost_enabled=False,
                price_margin_percent=0,
                theme_code="default",
                updated_by="system-bootstrap",
            )
        )
    else:
        db.add(
            CompanyProfileSetting(
                legal_name="Hollywood Pacas",
                trade_name="Hollywood Pacas",
                app_title="ERP Hollywood Pacas",
                sidebar_subtitle="ERP Central",
                website="http://hollywoodpacas.com.ni",
                ruc="",
                phone="8900-0300",
                address="Managua, De los semaforos del colonial 10 vrs. al lago frente al pillin.",
                email="admin@hollywoodpacas.com",
                logo_url="/static/logo_hollywood.png",
                pos_logo_url="/static/logo_hollywood.png",
                login_logo_url="/static/logo_hollywood.png",
                favicon_url="/static/favicon.ico",
                inventory_cs_only=False,
                weighted_inventory_enabled=False,
                weighted_sales_enabled=False,
                recipe_explosion_on_ingreso=False,
                multi_branch_enabled=multi_branch_enabled,
                price_auto_from_cost_enabled=False,
                price_margin_percent=0,
                theme_code="default",
                updated_by="system-bootstrap",
            )
        )
    db.commit()


def init_db() -> None:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    inspector = inspect(engine)
    if "users" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("users")}
        if "default_branch_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN default_branch_id INTEGER"))
        if "default_bodega_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN default_bodega_id INTEGER"))
    if "branches" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("branches")}
        if "activo" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE branches ADD COLUMN activo BOOLEAN DEFAULT TRUE"))
                conn.execute(text("UPDATE branches SET activo = TRUE WHERE activo IS NULL"))
        if "company_name" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE branches ADD COLUMN company_name VARCHAR(120)"))
        if "ruc" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE branches ADD COLUMN ruc VARCHAR(40)"))
        if "telefono" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE branches ADD COLUMN telefono VARCHAR(40)"))
        if "direccion" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE branches ADD COLUMN direccion VARCHAR(240)"))
    if "clientes" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("clientes")}
        if "identificacion" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE clientes ADD COLUMN identificacion VARCHAR(40)"))
    if "ventas_facturas" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ventas_facturas")}
        if "bodega_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_facturas ADD COLUMN bodega_id INTEGER"))
        if "estado" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_facturas ADD COLUMN estado VARCHAR(20) DEFAULT 'ACTIVA'"))
        if "reversion_motivo" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_facturas ADD COLUMN reversion_motivo VARCHAR(300)"))
        if "revertida_por" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_facturas ADD COLUMN revertida_por VARCHAR(160)"))
        if "revertida_at" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_facturas ADD COLUMN revertida_at TIMESTAMP"))
        if "estado_cobranza" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_facturas ADD COLUMN estado_cobranza VARCHAR(20) DEFAULT 'PENDIENTE'"))
        if "condicion_venta" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_facturas ADD COLUMN condicion_venta VARCHAR(20) DEFAULT 'CONTADO'"))
                conn.execute(text("UPDATE ventas_facturas SET condicion_venta = 'CONTADO' WHERE condicion_venta IS NULL"))
    if "ventas_reversion_tokens" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ventas_reversion_tokens")}
        if "action_type" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_reversion_tokens ADD COLUMN action_type VARCHAR(40) DEFAULT 'REVERSION'"))
                conn.execute(text("UPDATE ventas_reversion_tokens SET action_type = 'REVERSION' WHERE action_type IS NULL"))
        if "vendedor_nuevo_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_reversion_tokens ADD COLUMN vendedor_nuevo_id INTEGER"))
    if "egresos_inventario" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("egresos_inventario")}
        if "bodega_destino_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE egresos_inventario ADD COLUMN bodega_destino_id INTEGER"))
    if "egreso_items" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("egreso_items")}
        if "variante_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE egreso_items ADD COLUMN variante_id INTEGER"))
    if "ingreso_items" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ingreso_items")}
        if "variante_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ingreso_items ADD COLUMN variante_id INTEGER"))
    if "ventas_items" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ventas_items")}
        if "combo_role" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_items ADD COLUMN combo_role VARCHAR(20)"))
        if "combo_group" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_items ADD COLUMN combo_group VARCHAR(60)"))
        if "variante_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_items ADD COLUMN variante_id INTEGER"))
    if "ventas_preventas_items" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ventas_preventas_items")}
        if "combo_role" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_preventas_items ADD COLUMN combo_role VARCHAR(20)"))
        if "combo_group" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_preventas_items ADD COLUMN combo_group VARCHAR(60)"))
    if "cobranza_abonos" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("cobranza_abonos")}
        if "secuencia" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE cobranza_abonos ADD COLUMN secuencia INTEGER DEFAULT 1"))
        if "numero" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE cobranza_abonos ADD COLUMN numero VARCHAR(20)"))
        if "tipo_mov" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE cobranza_abonos ADD COLUMN tipo_mov VARCHAR(20) DEFAULT 'ABONO'"))
                conn.execute(text("UPDATE cobranza_abonos SET tipo_mov = 'ABONO' WHERE tipo_mov IS NULL OR tipo_mov = ''"))
    if "recibos_rubros" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("recibos_rubros")}
        if "cuenta_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE recibos_rubros ADD COLUMN cuenta_id INTEGER"))
    if "cuentas_contables" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("cuentas_contables")}
        if "tipo" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE cuentas_contables ADD COLUMN tipo VARCHAR(20)"))
    if "depositos_clientes" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("depositos_clientes")}
        if "metodo" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE depositos_clientes ADD COLUMN metodo VARCHAR(40) DEFAULT 'DEPOSITO_BANCARIO'"))
                conn.execute(text("UPDATE depositos_clientes SET metodo = 'DEPOSITO_BANCARIO' WHERE metodo IS NULL"))
    if "cobranza_abonos" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("cobranza_abonos")}
        if "afecta_caja" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE cobranza_abonos ADD COLUMN afecta_caja BOOLEAN DEFAULT FALSE"))
                conn.execute(text("UPDATE cobranza_abonos SET afecta_caja = FALSE WHERE afecta_caja IS NULL"))
    if "marcas" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("marcas")}
        if "abreviatura" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE marcas ADD COLUMN abreviatura VARCHAR(20)"))
                conn.execute(text("UPDATE marcas SET abreviatura = '' WHERE abreviatura IS NULL"))
    if "productos" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("productos")}
        for idx in (4, 5, 6, 7):
            cs_col = f"precio_venta{idx}"
            usd_col = f"precio_venta{idx}_usd"
            if cs_col not in columns:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE productos ADD COLUMN {cs_col} NUMERIC(12,2) DEFAULT 0"))
                    conn.execute(text(f"UPDATE productos SET {cs_col} = 0 WHERE {cs_col} IS NULL"))
            if usd_col not in columns:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE productos ADD COLUMN {usd_col} NUMERIC(12,2)"))
                    conn.execute(text(f"UPDATE productos SET {usd_col} = 0 WHERE {usd_col} IS NULL"))
        if "es_por_peso" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE productos ADD COLUMN es_por_peso BOOLEAN DEFAULT FALSE"))
                conn.execute(text("UPDATE productos SET es_por_peso = FALSE WHERE es_por_peso IS NULL"))
        if "tipo_producto" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE productos ADD COLUMN tipo_producto VARCHAR(30) DEFAULT 'DIRECTO'"))
                conn.execute(text("UPDATE productos SET tipo_producto = 'DIRECTO' WHERE tipo_producto IS NULL OR tipo_producto = ''"))
        if "unidad_medida_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE productos ADD COLUMN unidad_medida_id INTEGER"))
        if "image_url" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE productos ADD COLUMN image_url VARCHAR(260)"))
    if "unidades_medida" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("unidades_medida")}
        if "abreviatura" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE unidades_medida ADD COLUMN abreviatura VARCHAR(20) DEFAULT 'lb'"))
                conn.execute(text("UPDATE unidades_medida SET abreviatura = 'lb' WHERE abreviatura IS NULL OR abreviatura = ''"))
    if "pos_print_settings" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("pos_print_settings")}
        if "sumatra_path" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE pos_print_settings ADD COLUMN sumatra_path VARCHAR(260)"))
        if "roc_printer_name" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE pos_print_settings ADD COLUMN roc_printer_name VARCHAR(120)"))
        if "roc_copies" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE pos_print_settings ADD COLUMN roc_copies INTEGER"))
        if "roc_auto_print" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE pos_print_settings ADD COLUMN roc_auto_print BOOLEAN DEFAULT FALSE"))
        if "cierre_printer_name" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE pos_print_settings ADD COLUMN cierre_printer_name VARCHAR(120)"))
        if "cierre_copies" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE pos_print_settings ADD COLUMN cierre_copies INTEGER"))
        if "cierre_auto_print" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE pos_print_settings ADD COLUMN cierre_auto_print BOOLEAN DEFAULT FALSE"))
    if "mobile_push_subscriptions" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("mobile_push_subscriptions")}
        if "branch_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE mobile_push_subscriptions ADD COLUMN branch_id INTEGER"))
        if "bodega_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE mobile_push_subscriptions ADD COLUMN bodega_id INTEGER"))
        if "user_agent" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE mobile_push_subscriptions ADD COLUMN user_agent VARCHAR(255)"))
        if "activo" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE mobile_push_subscriptions ADD COLUMN activo BOOLEAN DEFAULT TRUE"))
                conn.execute(text("UPDATE mobile_push_subscriptions SET activo = TRUE WHERE activo IS NULL"))
        if "last_seen_at" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE mobile_push_subscriptions ADD COLUMN last_seen_at TIMESTAMP"))
        if "updated_at" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE mobile_push_subscriptions ADD COLUMN updated_at TIMESTAMP DEFAULT NOW()"))
    if "users" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("users")}
        if "vendedor_id" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN vendedor_id INTEGER"))
    if "company_profile_settings" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("company_profile_settings")}
        if "ruc" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN ruc VARCHAR(40)"))
                conn.execute(text("UPDATE company_profile_settings SET ruc = '' WHERE ruc IS NULL"))
        if "pos_logo_url" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN pos_logo_url VARCHAR(260)"))
                conn.execute(
                    text(
                        "UPDATE company_profile_settings SET pos_logo_url = COALESCE(NULLIF(logo_url, ''), '/static/logo_hollywood.png')"
                    )
                )
        if "login_logo_url" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN login_logo_url VARCHAR(260)"))
                conn.execute(
                    text(
                        "UPDATE company_profile_settings "
                        "SET login_logo_url = COALESCE(NULLIF(pos_logo_url, ''), NULLIF(logo_url, ''), '/static/logo_hollywood.png') "
                        "WHERE login_logo_url IS NULL OR login_logo_url = ''"
                    )
                )
        if "inventory_cs_only" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN inventory_cs_only BOOLEAN DEFAULT FALSE"))
                conn.execute(text("UPDATE company_profile_settings SET inventory_cs_only = FALSE WHERE inventory_cs_only IS NULL"))
        if "recipe_explosion_on_ingreso" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN recipe_explosion_on_ingreso BOOLEAN DEFAULT FALSE"))
                conn.execute(
                    text(
                        "UPDATE company_profile_settings SET recipe_explosion_on_ingreso = FALSE "
                        "WHERE recipe_explosion_on_ingreso IS NULL"
                    )
                )
        if "weighted_inventory_enabled" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN weighted_inventory_enabled BOOLEAN DEFAULT FALSE"))
                conn.execute(
                    text(
                        "UPDATE company_profile_settings SET weighted_inventory_enabled = FALSE "
                        "WHERE weighted_inventory_enabled IS NULL"
                    )
                )
        if "weighted_sales_enabled" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN weighted_sales_enabled BOOLEAN DEFAULT FALSE"))
                conn.execute(
                    text(
                        "UPDATE company_profile_settings SET weighted_sales_enabled = FALSE "
                        "WHERE weighted_sales_enabled IS NULL"
                    )
                )
        if "multi_branch_enabled" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN multi_branch_enabled BOOLEAN DEFAULT TRUE"))
                default_multi = "FALSE" if get_active_company_key() in {"comestibles", "barrera", "bdtrend"} else "TRUE"
                conn.execute(text(f"UPDATE company_profile_settings SET multi_branch_enabled = {default_multi} WHERE multi_branch_enabled IS NULL"))
        if "price_auto_from_cost_enabled" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN price_auto_from_cost_enabled BOOLEAN DEFAULT FALSE"))
                conn.execute(
                    text(
                        "UPDATE company_profile_settings SET price_auto_from_cost_enabled = FALSE WHERE price_auto_from_cost_enabled IS NULL"
                    )
                )
        if "price_margin_percent" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN price_margin_percent INTEGER DEFAULT 0"))
                conn.execute(text("UPDATE company_profile_settings SET price_margin_percent = 0 WHERE price_margin_percent IS NULL"))
        if "theme_code" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE company_profile_settings ADD COLUMN theme_code VARCHAR(40) DEFAULT 'default'"))
                conn.execute(text("UPDATE company_profile_settings SET theme_code = 'default' WHERE theme_code IS NULL OR theme_code = ''"))
    if "email_recipients" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("email_recipients")}
        if "sales_close_active" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE email_recipients ADD COLUMN sales_close_active BOOLEAN DEFAULT FALSE"))
                conn.execute(text("UPDATE email_recipients SET sales_close_active = FALSE WHERE sales_close_active IS NULL"))
    if "bodega_requisa_cierres" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("bodega_requisa_cierres")}
        if "movement_type" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE bodega_requisa_cierres ADD COLUMN movement_type VARCHAR(30) DEFAULT 'sales_out'"))
                conn.execute(
                    text(
                        "UPDATE bodega_requisa_cierres SET movement_type = 'sales_out' "
                        "WHERE movement_type IS NULL OR movement_type = ''"
                    )
                )
        if "anulada" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE bodega_requisa_cierres ADD COLUMN anulada BOOLEAN DEFAULT FALSE"))
                conn.execute(text("UPDATE bodega_requisa_cierres SET anulada = FALSE WHERE anulada IS NULL"))
        if "anulada_motivo" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE bodega_requisa_cierres ADD COLUMN anulada_motivo VARCHAR(500)"))
        if "anulada_por" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE bodega_requisa_cierres ADD COLUMN anulada_por VARCHAR(160)"))
        if "anulada_at" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE bodega_requisa_cierres ADD COLUMN anulada_at TIMESTAMP"))
    if "bodega_requisa_drafts" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("bodega_requisa_drafts")}
        if "movement_type" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE bodega_requisa_drafts ADD COLUMN movement_type VARCHAR(30) DEFAULT 'sales_out'"))
                conn.execute(
                    text(
                        "UPDATE bodega_requisa_drafts SET movement_type = 'sales_out' "
                        "WHERE movement_type IS NULL OR movement_type = ''"
                    )
                )
    if "vendedor_bodegas" not in inspector.get_table_names():
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE vendedor_bodegas (
                        id SERIAL PRIMARY KEY,
                        vendedor_id INTEGER NOT NULL REFERENCES vendedores(id),
                        bodega_id INTEGER NOT NULL REFERENCES bodegas(id),
                        is_default BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW(),
                        CONSTRAINT uq_vendedor_bodega UNIQUE (vendedor_id, bodega_id)
                    )
                    """
                )
            )
    if "restaurant_orders" in inspector.get_table_names():
        restaurant_cols = {col["name"] for col in inspector.get_columns("restaurant_orders")}
        if "table_id" not in restaurant_cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE restaurant_orders ADD COLUMN table_id INTEGER NULL REFERENCES restaurant_tables(id)"))
    if "restaurant_tables" in inspector.get_table_names():
        restaurant_table_cols = {col["name"] for col in inspector.get_columns("restaurant_tables")}
        if "pos_x" not in restaurant_table_cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE restaurant_tables ADD COLUMN pos_x INTEGER DEFAULT 10"))
                conn.execute(text("UPDATE restaurant_tables SET pos_x = 10 WHERE pos_x IS NULL"))
        if "pos_y" not in restaurant_table_cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE restaurant_tables ADD COLUMN pos_y INTEGER DEFAULT 10"))
                conn.execute(text("UPDATE restaurant_tables SET pos_y = 10 WHERE pos_y IS NULL"))
        if "width_units" not in restaurant_table_cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE restaurant_tables ADD COLUMN width_units INTEGER DEFAULT 1"))
                conn.execute(text("UPDATE restaurant_tables SET width_units = 1 WHERE width_units IS NULL"))
        if "height_units" not in restaurant_table_cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE restaurant_tables ADD COLUMN height_units INTEGER DEFAULT 1"))
                conn.execute(text("UPDATE restaurant_tables SET height_units = 1 WHERE height_units IS NULL"))
    db = get_session_local()()
    try:
        _seed_unidades_medida(db)
        _seed_roles(db)
        _seed_permissions(db)
        _seed_branches(db)
        _seed_role_permissions(db)
        _seed_admin(db)
        _seed_admin_branch_access(db)
        _seed_lineas(db)
        _seed_segmentos(db)
        _seed_marcas(db)
        _seed_bodegas(db)
        _seed_ingreso_tipos(db)
        _seed_egreso_tipos(db)
        _seed_formas_pago(db)
        _seed_bancos(db)
        _seed_cuentas_bancarias(db)
        _seed_vendedores(db)
        _seed_cuentas_contables(db)
        _seed_accounting_voucher_types(db)
        _seed_accounting_policy_settings(db)
        _seed_recibos_rubros(db)
        _seed_recibos_motivos(db)
        _seed_pos_print_settings(db)
        _seed_email_config(db)
        _seed_email_recipients(db)
        _seed_sales_interface_settings(db)
        _seed_company_profile_settings(db)
        _seed_restaurant_tables(db)
        _seed_restaurant_demo_products(db)
    finally:
        db.close()
