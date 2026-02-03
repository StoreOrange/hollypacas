from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from ..database import Base
from .user import Branch


class Cliente(Base):
    __tablename__ = "clientes"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(160), nullable=False, unique=True)
    identificacion = Column(String(40), nullable=True)
    telefono = Column(String(40), nullable=True)
    email = Column(String(120), nullable=True)
    direccion = Column(String(200), nullable=True)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())


class Vendedor(Base):
    __tablename__ = "vendedores"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(160), nullable=False, unique=True)
    telefono = Column(String(40), nullable=True)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

    assignments = relationship("VendedorBodega", back_populates="vendedor", cascade="all, delete-orphan")


class VendedorBodega(Base):
    __tablename__ = "vendedor_bodegas"
    __table_args__ = (UniqueConstraint("vendedor_id", "bodega_id", name="uq_vendedor_bodega"),)

    id = Column(Integer, primary_key=True, index=True)
    vendedor_id = Column(Integer, ForeignKey("vendedores.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

    vendedor = relationship("Vendedor", back_populates="assignments")
    bodega = relationship("Bodega")


class FormaPago(Base):
    __tablename__ = "formas_pago"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(80), nullable=False, unique=True)
    created_at = Column(DateTime, server_default=func.now())


class Banco(Base):
    __tablename__ = "bancos"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(80), nullable=False, unique=True)
    created_at = Column(DateTime, server_default=func.now())


class CuentaBancaria(Base):
    __tablename__ = "cuentas_bancarias"

    id = Column(Integer, primary_key=True, index=True)
    banco_id = Column(Integer, ForeignKey("bancos.id"), nullable=False)
    moneda = Column(String(10), nullable=False)
    cuenta = Column(String(60), nullable=True)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

    banco = relationship("Banco")


class PosPrintSetting(Base):
    __tablename__ = "pos_print_settings"

    id = Column(Integer, primary_key=True, index=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False, unique=True)
    printer_name = Column(String(120), nullable=False, default="HP Receipt")
    copies = Column(Integer, nullable=False, default=1)
    auto_print = Column(Boolean, default=True)
    roc_printer_name = Column(String(120), nullable=True)
    roc_copies = Column(Integer, nullable=True)
    roc_auto_print = Column(Boolean, default=False)
    cierre_printer_name = Column(String(120), nullable=True)
    cierre_copies = Column(Integer, nullable=True)
    cierre_auto_print = Column(Boolean, default=False)
    sumatra_path = Column(String(260), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    branch = relationship("Branch")


class EmailConfig(Base):
    __tablename__ = "email_config"

    id = Column(Integer, primary_key=True, index=True)
    sender_email = Column(String(160), nullable=False, default="orangetectec@zohomail.com")
    sender_name = Column(String(160), nullable=True)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())


class NotificationRecipient(Base):
    __tablename__ = "email_recipients"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(160), nullable=False, unique=True)
    name = Column(String(160), nullable=True)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())


class ReversionToken(Base):
    __tablename__ = "ventas_reversion_tokens"

    id = Column(Integer, primary_key=True, index=True)
    factura_id = Column(Integer, ForeignKey("ventas_facturas.id"), nullable=False)
    token = Column(String(20), nullable=False)
    motivo = Column(String(300), nullable=False)
    solicitado_por = Column(String(160), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)

    factura = relationship("VentaFactura")


class VentaFactura(Base):
    __tablename__ = "ventas_facturas"

    id = Column(Integer, primary_key=True, index=True)
    secuencia = Column(Integer, nullable=False, default=1)
    numero = Column(String(20), nullable=False, unique=True)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=True)
    cliente_id = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    vendedor_id = Column(Integer, ForeignKey("vendedores.id"), nullable=True)
    fecha = Column(DateTime, server_default=func.now())
    moneda = Column(String(10), nullable=False)
    tasa_cambio = Column(Numeric(12, 4), nullable=True)
    total_usd = Column(Numeric(14, 2), default=0)
    total_cs = Column(Numeric(14, 2), default=0)
    total_items = Column(Numeric(14, 2), default=0)
    usuario_registro = Column(String(120), nullable=True)
    estado = Column(String(20), nullable=False, default="ACTIVA")
    estado_cobranza = Column(String(20), nullable=False, default="PENDIENTE")
    reversion_motivo = Column(String(300), nullable=True)
    revertida_por = Column(String(160), nullable=True)
    revertida_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    cliente = relationship("Cliente")
    vendedor = relationship("Vendedor")
    bodega = relationship("Bodega")
    items = relationship("VentaItem", back_populates="factura")
    pagos = relationship("VentaPago", back_populates="factura")
    abonos = relationship("CobranzaAbono", back_populates="factura")


class VentaItem(Base):
    __tablename__ = "ventas_items"

    id = Column(Integer, primary_key=True, index=True)
    factura_id = Column(Integer, ForeignKey("ventas_facturas.id"), nullable=False)
    producto_id = Column(Integer, ForeignKey("productos.id"), nullable=False)
    cantidad = Column(Numeric(14, 2), default=0)
    precio_unitario_usd = Column(Numeric(14, 2), default=0)
    precio_unitario_cs = Column(Numeric(14, 2), default=0)
    subtotal_usd = Column(Numeric(14, 2), default=0)
    subtotal_cs = Column(Numeric(14, 2), default=0)
    combo_role = Column(String(20), nullable=True)
    combo_group = Column(String(60), nullable=True)

    factura = relationship("VentaFactura", back_populates="items")
    producto = relationship("Producto")


class VentaPago(Base):
    __tablename__ = "ventas_pagos"

    id = Column(Integer, primary_key=True, index=True)
    factura_id = Column(Integer, ForeignKey("ventas_facturas.id"), nullable=False)
    forma_pago_id = Column(Integer, ForeignKey("formas_pago.id"), nullable=False)
    banco_id = Column(Integer, ForeignKey("bancos.id"), nullable=True)
    cuenta_id = Column(Integer, ForeignKey("cuentas_bancarias.id"), nullable=True)
    monto_usd = Column(Numeric(14, 2), default=0)
    monto_cs = Column(Numeric(14, 2), default=0)

    factura = relationship("VentaFactura", back_populates="pagos")
    forma_pago = relationship("FormaPago")
    banco = relationship("Banco")
    cuenta = relationship("CuentaBancaria")


class CobranzaAbono(Base):
    __tablename__ = "cobranza_abonos"

    id = Column(Integer, primary_key=True, index=True)
    factura_id = Column(Integer, ForeignKey("ventas_facturas.id"), nullable=False)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    secuencia = Column(Integer, nullable=False, default=1)
    numero = Column(String(20), nullable=False)
    fecha = Column(Date, nullable=False)
    moneda = Column(String(10), nullable=False)
    tasa_cambio = Column(Numeric(12, 4), nullable=True)
    monto_usd = Column(Numeric(14, 2), default=0)
    monto_cs = Column(Numeric(14, 2), default=0)
    observacion = Column(String(300), nullable=True)
    usuario_registro = Column(String(120), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    factura = relationship("VentaFactura", back_populates="abonos")
    branch = relationship("Branch")
    bodega = relationship("Bodega")


class ReciboRubro(Base):
    __tablename__ = "recibos_rubros"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(120), nullable=False, unique=True)
    cuenta_id = Column(Integer, ForeignKey("cuentas_contables.id"), nullable=True)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

    cuenta = relationship("CuentaContable")


class ReciboMotivo(Base):
    __tablename__ = "recibos_motivos"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(160), nullable=False, unique=True)
    tipo = Column(String(20), nullable=False)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())


class ReciboCaja(Base):
    __tablename__ = "recibos_caja"

    id = Column(Integer, primary_key=True, index=True)
    secuencia = Column(Integer, nullable=False, default=1)
    numero = Column(String(20), nullable=False, unique=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    tipo = Column(String(20), nullable=False)
    rubro_id = Column(Integer, ForeignKey("recibos_rubros.id"), nullable=False)
    motivo_id = Column(Integer, ForeignKey("recibos_motivos.id"), nullable=False)
    descripcion = Column(String(400), nullable=True)
    fecha = Column(Date, nullable=False)
    moneda = Column(String(10), nullable=False)
    tasa_cambio = Column(Numeric(12, 4), nullable=True)
    monto_usd = Column(Numeric(14, 2), default=0)
    monto_cs = Column(Numeric(14, 2), default=0)
    afecta_caja = Column(Boolean, default=True)
    usuario_registro = Column(String(120), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    branch = relationship("Branch")
    bodega = relationship("Bodega")
    rubro = relationship("ReciboRubro")
    motivo = relationship("ReciboMotivo")


class DepositoCliente(Base):
    __tablename__ = "depositos_clientes"

    id = Column(Integer, primary_key=True, index=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    vendedor_id = Column(Integer, ForeignKey("vendedores.id"), nullable=False)
    banco_id = Column(Integer, ForeignKey("bancos.id"), nullable=False)
    cuenta_id = Column(Integer, ForeignKey("cuentas_bancarias.id"), nullable=True)
    fecha = Column(Date, nullable=False)
    moneda = Column(String(10), nullable=False)
    tasa_cambio = Column(Numeric(12, 4), nullable=True)
    monto_usd = Column(Numeric(14, 2), default=0)
    monto_cs = Column(Numeric(14, 2), default=0)
    observacion = Column(String(400), nullable=True)
    usuario_registro = Column(String(120), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    branch = relationship("Branch")
    bodega = relationship("Bodega")
    vendedor = relationship("Vendedor")
    banco = relationship("Banco")
    cuenta = relationship("CuentaBancaria")


class CajaDiaria(Base):
    __tablename__ = "caja_diaria"
    __table_args__ = (UniqueConstraint("branch_id", "bodega_id", "fecha", name="uq_caja_diaria"),)

    id = Column(Integer, primary_key=True, index=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    fecha = Column(Date, nullable=False)
    saldo_usd = Column(Numeric(14, 2), default=0)
    saldo_cs = Column(Numeric(14, 2), default=0)
    created_at = Column(DateTime, server_default=func.now())


class CierreCaja(Base):
    __tablename__ = "cierres_caja"

    id = Column(Integer, primary_key=True, index=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    fecha = Column(Date, nullable=False)
    detalle_cs = Column(Text, nullable=True)
    detalle_usd = Column(Text, nullable=True)
    total_efectivo_cs = Column(Numeric(14, 2), default=0)
    total_efectivo_usd = Column(Numeric(14, 2), default=0)
    total_efectivo_usd_equiv = Column(Numeric(14, 2), default=0)
    total_ventas_usd = Column(Numeric(14, 2), default=0)
    total_ingresos_usd = Column(Numeric(14, 2), default=0)
    total_egresos_usd = Column(Numeric(14, 2), default=0)
    total_depositos_usd = Column(Numeric(14, 2), default=0)
    total_creditos_usd = Column(Numeric(14, 2), default=0)
    total_calculado_usd = Column(Numeric(14, 2), default=0)
    diferencia_usd = Column(Numeric(14, 2), default=0)
    usuario_registro = Column(String(160), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    branch = relationship("Branch")
    bodega = relationship("Bodega")

    branch = relationship("Branch")
    bodega = relationship("Bodega")


class CuentaContable(Base):
    __tablename__ = "cuentas_contables"

    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String(20), nullable=False, unique=True)
    nombre = Column(String(160), nullable=False)
    tipo = Column(String(20), nullable=False)  # BALANCE / RESULTADO
    naturaleza = Column(String(10), nullable=False)  # DEBE / HABER
    parent_id = Column(Integer, ForeignKey("cuentas_contables.id"), nullable=True)
    nivel = Column(Integer, nullable=False, default=1)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

    parent = relationship("CuentaContable", remote_side=[id])
