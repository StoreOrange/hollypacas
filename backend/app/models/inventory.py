from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from ..database import Base
from .user import Branch


class Linea(Base):
    __tablename__ = "lineas"

    id = Column(Integer, primary_key=True, index=True)
    cod_linea = Column(String(50), unique=True, nullable=False)
    linea = Column(String(120), nullable=False)
    activo = Column(Boolean, default=True)
    registro = Column(DateTime, server_default=func.now())

    productos = relationship("Producto", back_populates="linea")


class Segmento(Base):
    __tablename__ = "segmentos"

    id = Column(Integer, primary_key=True, index=True)
    segmento = Column(String(120), unique=True, nullable=False)
    registro = Column(DateTime, server_default=func.now())

    productos = relationship("Producto", back_populates="segmento")


class Producto(Base):
    __tablename__ = "productos"

    id = Column(Integer, primary_key=True, index=True)
    cod_producto = Column(String(60), unique=True, nullable=False)
    descripcion = Column(String(200), nullable=False)
    segmento_id = Column(Integer, ForeignKey("segmentos.id"), nullable=True)
    linea_id = Column(Integer, ForeignKey("lineas.id"), nullable=True)
    marca = Column(String(80), nullable=True)
    precio_venta1 = Column(Numeric(12, 2), default=0)
    precio_venta2 = Column(Numeric(12, 2), default=0)
    precio_venta3 = Column(Numeric(12, 2), default=0)
    precio_venta1_usd = Column(Numeric(12, 2), nullable=True)
    precio_venta2_usd = Column(Numeric(12, 2), nullable=True)
    precio_venta3_usd = Column(Numeric(12, 2), nullable=True)
    tasa_cambio = Column(Numeric(12, 4), nullable=True)
    activo = Column(Boolean, default=True)
    servicio_producto = Column(Boolean, default=False)
    costo_producto = Column(Numeric(12, 2), default=0)
    referencia_producto = Column(String(120), nullable=True)
    usuario_registro = Column(String(80), nullable=True)
    maquina_registro = Column(String(80), nullable=True)
    registro = Column(DateTime, server_default=func.now())
    ultima_modificacion = Column(DateTime, server_default=func.now(), onupdate=func.now())

    linea = relationship("Linea", back_populates="productos")
    segmento = relationship("Segmento", back_populates="productos")
    saldo = relationship("SaldoProducto", back_populates="producto", uselist=False)
    combo_children = relationship(
        "ProductoCombo",
        back_populates="parent",
        cascade="all, delete-orphan",
        foreign_keys="ProductoCombo.parent_producto_id",
    )


class ProductoCombo(Base):
    __tablename__ = "producto_combos"

    id = Column(Integer, primary_key=True, index=True)
    parent_producto_id = Column(Integer, ForeignKey("productos.id"), nullable=False)
    child_producto_id = Column(Integer, ForeignKey("productos.id"), nullable=False)
    cantidad = Column(Numeric(12, 2), default=1)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

    parent = relationship("Producto", foreign_keys=[parent_producto_id], back_populates="combo_children")
    child = relationship("Producto", foreign_keys=[child_producto_id])


class SaldoProducto(Base):
    __tablename__ = "saldos_productos"

    id = Column(Integer, primary_key=True, index=True)
    producto_id = Column(Integer, ForeignKey("productos.id"), unique=True)
    existencia = Column(Numeric(14, 2), default=0)

    producto = relationship("Producto", back_populates="saldo")


class ExchangeRate(Base):
    __tablename__ = "exchange_rates"

    id = Column(Integer, primary_key=True, index=True)
    effective_date = Column(Date, nullable=False)
    period = Column(String(20), nullable=False)
    rate = Column(Numeric(12, 4), nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class Bodega(Base):
    __tablename__ = "bodegas"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(40), unique=True, nullable=False)
    name = Column(String(120), nullable=False)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

    branch = relationship(Branch)


class Proveedor(Base):
    __tablename__ = "proveedores"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(160), unique=True, nullable=False)
    tipo = Column(String(40), nullable=True)
    activo = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())


class IngresoTipo(Base):
    __tablename__ = "ingreso_tipos"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(120), unique=True, nullable=False)
    requiere_proveedor = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())


class IngresoInventario(Base):
    __tablename__ = "ingresos_inventario"

    id = Column(Integer, primary_key=True, index=True)
    tipo_id = Column(Integer, ForeignKey("ingreso_tipos.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    proveedor_id = Column(Integer, ForeignKey("proveedores.id"), nullable=True)
    fecha = Column(Date, nullable=False)
    moneda = Column(String(10), nullable=False)
    tasa_cambio = Column(Numeric(12, 4), nullable=True)
    total_usd = Column(Numeric(14, 2), default=0)
    total_cs = Column(Numeric(14, 2), default=0)
    observacion = Column(String(300), nullable=True)
    usuario_registro = Column(String(120), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    tipo = relationship("IngresoTipo")
    bodega = relationship("Bodega")
    proveedor = relationship("Proveedor")
    items = relationship("IngresoItem", back_populates="ingreso")


class IngresoItem(Base):
    __tablename__ = "ingreso_items"

    id = Column(Integer, primary_key=True, index=True)
    ingreso_id = Column(Integer, ForeignKey("ingresos_inventario.id"), nullable=False)
    producto_id = Column(Integer, ForeignKey("productos.id"), nullable=False)
    cantidad = Column(Numeric(14, 2), default=0)
    costo_unitario_usd = Column(Numeric(14, 2), default=0)
    costo_unitario_cs = Column(Numeric(14, 2), default=0)
    subtotal_usd = Column(Numeric(14, 2), default=0)
    subtotal_cs = Column(Numeric(14, 2), default=0)

    ingreso = relationship("IngresoInventario", back_populates="items")
    producto = relationship("Producto")


class EgresoTipo(Base):
    __tablename__ = "egreso_tipos"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(120), unique=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class EgresoInventario(Base):
    __tablename__ = "egresos_inventario"

    id = Column(Integer, primary_key=True, index=True)
    tipo_id = Column(Integer, ForeignKey("egreso_tipos.id"), nullable=False)
    bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=False)
    fecha = Column(Date, nullable=False)
    moneda = Column(String(10), nullable=False)
    tasa_cambio = Column(Numeric(12, 4), nullable=True)
    total_usd = Column(Numeric(14, 2), default=0)
    total_cs = Column(Numeric(14, 2), default=0)
    observacion = Column(String(300), nullable=True)
    usuario_registro = Column(String(120), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    tipo = relationship("EgresoTipo")
    bodega = relationship("Bodega")
    items = relationship("EgresoItem", back_populates="egreso")


class EgresoItem(Base):
    __tablename__ = "egreso_items"

    id = Column(Integer, primary_key=True, index=True)
    egreso_id = Column(Integer, ForeignKey("egresos_inventario.id"), nullable=False)
    producto_id = Column(Integer, ForeignKey("productos.id"), nullable=False)
    cantidad = Column(Numeric(14, 2), default=0)
    costo_unitario_usd = Column(Numeric(14, 2), default=0)
    costo_unitario_cs = Column(Numeric(14, 2), default=0)
    subtotal_usd = Column(Numeric(14, 2), default=0)
    subtotal_cs = Column(Numeric(14, 2), default=0)

    egreso = relationship("EgresoInventario", back_populates="items")
    producto = relationship("Producto")
