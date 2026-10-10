"""Customer returns and indivisible exchange credits, in cordobas."""
from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship
from ..database import Base


class CustomerReturn(Base):
    __tablename__ = 'customer_returns'
    __table_args__ = (
        CheckConstraint('cantidad > 0', name='ck_return_quantity'),
        CheckConstraint('monto_cs > 0', name='ck_return_amount'),
        CheckConstraint("tipo IN ('CANJE', 'DINERO')", name='ck_return_type'),
    )
    id = Column(Integer, primary_key=True)
    operation_key = Column(String(32), nullable=False, unique=True)
    factura_id = Column(Integer, ForeignKey('ventas_facturas.id'), nullable=False, index=True)
    item_id = Column(Integer, ForeignKey('ventas_items.id'), nullable=False, index=True)
    cliente_id = Column(Integer, ForeignKey('clientes.id'), nullable=False, index=True)
    bodega_id = Column(Integer, ForeignKey('bodegas.id'), nullable=False, index=True)
    cantidad = Column(Numeric(14, 2), nullable=False)
    monto_cs = Column(Numeric(14, 2), nullable=False)
    costo_cs = Column(Numeric(14, 2), nullable=False)
    tipo = Column(String(12), nullable=False)
    motivo = Column(String(300), nullable=False)
    usuario_registro = Column(String(120), nullable=False)
    created_at = Column(DateTime, nullable=False)
    ingreso_id = Column(Integer, ForeignKey('ingresos_inventario.id'), nullable=False)
    recibo_id = Column(Integer, ForeignKey('recibos_caja.id'))
    factura_canje_id = Column(Integer, ForeignKey('ventas_facturas.id'), index=True)
    canjeado_at = Column(DateTime)
    canjeado_por = Column(String(120))

    factura = relationship('VentaFactura', foreign_keys=[factura_id])
    factura_canje = relationship('VentaFactura', foreign_keys=[factura_canje_id])
    item = relationship('VentaItem')
    cliente = relationship('Cliente')
    bodega = relationship('Bodega')
    recibo = relationship('ReciboCaja')

    @property
    def numero(self):
        return f'DEV-{self.id:06d}'

    @property
    def estado(self):
        if self.tipo == 'DINERO':
            return 'DINERO ENTREGADO'
        return 'APLICADO EN FACTURA' if self.factura_canje_id else 'ANTICIPO DISPONIBLE'
