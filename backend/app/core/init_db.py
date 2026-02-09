from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from ..config import settings
from ..database import Base, SessionLocal, engine
from ..models.user import Branch, Permission, Role, User
from ..models.inventory import Bodega, EgresoTipo, IngresoTipo, Linea, Segmento
from ..models.sales import (
    Banco,
    CuentaContable,
    CuentaBancaria,
    EmailConfig,
    ReciboMotivo,
    ReciboRubro,
    FormaPago,
    NotificationRecipient,
    PosPrintSetting,
    Vendedor,
)
from .security import hash_password


def _seed_roles(db: Session) -> None:
    role_names = ["administrador", "vendedor", "cajero", "seguridad"]
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
        "menu.finance",
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
        "access.inventory.productos",
        "access.finance",
        "access.finance.rates",
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
    branches = [
        (
            "central",
            "Central",
            "Hollywood Pacas",
            "0012202910068H",
            "8900-0300",
            "Managua, De los semaforos del colonial 10 vrs. al lago frente al pillin.",
        ),
        (
            "esteli",
            "Sucursal Esteli",
            "Hollywood Pacas",
            "0012202910068H",
            "8900-0300",
            "Esteli, De auto lote del Norte 7 cuadras al este.",
        ),
    ]
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
    db.commit()


def _seed_role_permissions(db: Session) -> None:
    role_names = ["administrador", "seguridad"]
    permissions = db.query(Permission).all()
    for role_name in role_names:
        role = db.query(Role).filter(Role.name == role_name).first()
        if role and permissions:
            role.permissions = permissions
    db.commit()


def _seed_admin_branch_access(db: Session) -> None:
    admin = db.query(User).filter(User.email == settings.ADMIN_EMAIL).first()
    if not admin:
        return
    central_branch = db.query(Branch).filter(Branch.code == "central").first()
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
            db.add(Linea(cod_linea=name, linea=name, activo=True))
    db.commit()


def _seed_segmentos(db: Session) -> None:
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


def _seed_bodegas(db: Session) -> None:
    branches = {branch.code: branch for branch in db.query(Branch).all()}
    bodegas = [
        ("central", "Central", "central"),
        ("esteli", "Esteli", "esteli"),
    ]
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
        "Merma",
        "Perdida",
        "Reposicion a Cliente",
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
    formas = ["Tarjeta", "Banco", "Efectivo", "Credito", "Anticipo"]
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


def _seed_recibos_rubros(db: Session) -> None:
    rubros = [
        "Gastos Operativos",
        "Gastos administrativos",
        "Gastos financieros",
        "Gastos de Ventas",
        "Otros gastos",
    ]
    existing = {r.nombre for r in db.query(ReciboRubro).all()}
    for nombre in rubros:
        if nombre not in existing:
            db.add(ReciboRubro(nombre=nombre, activo=True))
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
        db.add(EmailConfig(sender_email="orangetectec@zohomail.com", sender_name="Hollywood Pacas"))
        db.commit()


def _seed_email_recipients(db: Session) -> None:
    recipients = ["oddgarcia.samuel@gmail.com"]
    existing = {r.email for r in db.query(NotificationRecipient).all()}
    for email in recipients:
        if email not in existing:
            db.add(NotificationRecipient(email=email, active=True))
    db.commit()


def init_db() -> None:
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
    if "ventas_items" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("ventas_items")}
        if "combo_role" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_items ADD COLUMN combo_role VARCHAR(20)"))
        if "combo_group" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE ventas_items ADD COLUMN combo_group VARCHAR(60)"))
    if "cobranza_abonos" in inspector.get_table_names():
        columns = {column["name"] for column in inspector.get_columns("cobranza_abonos")}
        if "secuencia" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE cobranza_abonos ADD COLUMN secuencia INTEGER DEFAULT 1"))
        if "numero" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE cobranza_abonos ADD COLUMN numero VARCHAR(20)"))
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
    db = SessionLocal()
    try:
        _seed_roles(db)
        _seed_permissions(db)
        _seed_branches(db)
        _seed_role_permissions(db)
        _seed_admin(db)
        _seed_admin_branch_access(db)
        _seed_lineas(db)
        _seed_segmentos(db)
        _seed_bodegas(db)
        _seed_ingreso_tipos(db)
        _seed_egreso_tipos(db)
        _seed_formas_pago(db)
        _seed_bancos(db)
        _seed_cuentas_bancarias(db)
        _seed_vendedores(db)
        _seed_cuentas_contables(db)
        _seed_recibos_rubros(db)
        _seed_recibos_motivos(db)
        _seed_pos_print_settings(db)
        _seed_email_config(db)
        _seed_email_recipients(db)
    finally:
        db.close()
