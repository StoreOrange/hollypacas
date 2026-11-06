"""Auditable Holl branch requisitions; inventory is moved only on approval."""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, ForeignKey, CheckConstraint, Numeric, UniqueConstraint
from sqlalchemy.orm import relationship
from ..database import Base


class InventoryRequest(Base):
    __tablename__ = 'inventory_requests'
    __table_args__ = (
        CheckConstraint("status IN ('pending','active','cancelled')", name='ck_inventory_request_status'),
        CheckConstraint('source_id != destination_id', name='ck_inventory_request_route'),
        CheckConstraint("(status = 'active' AND egreso_id IS NOT NULL AND ingreso_id IS NOT NULL) OR (status != 'active' AND egreso_id IS NULL AND ingreso_id IS NULL)", name='ck_inventory_request_documents'),
    )
    id = Column(Integer, primary_key=True)
    operation_key = Column(String(32), nullable=False, unique=True)
    fecha = Column(Date, nullable=False)
    created_at = Column(DateTime, nullable=False)
    source_id = Column(Integer, ForeignKey('bodegas.id'), nullable=False)
    destination_id = Column(Integer, ForeignKey('bodegas.id'), nullable=False)
    requested_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    requester_name = Column(String(160), nullable=False)
    vendedor_id = Column(Integer, ForeignKey('vendedores.id'), nullable=False)
    seller_name = Column(String(160), nullable=False)
    status = Column(String(16), nullable=False, default='pending', index=True)
    observation = Column(String(1000), nullable=False, default='')
    decided_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    decider_name = Column(String(160), nullable=True)
    decided_at = Column(DateTime, nullable=True)
    decision_reason = Column(String(1000), nullable=False, default='')
    egreso_id = Column(Integer, ForeignKey('egresos_inventario.id'), unique=True, nullable=True)
    ingreso_id = Column(Integer, ForeignKey('ingresos_inventario.id'), unique=True, nullable=True)
    review_url = Column(String(500), nullable=False)
    email_state = Column(String(16), nullable=False, default='pending')
    email_error = Column(String(240), nullable=False, default='')
    notified_at = Column(DateTime, nullable=True)
    email_attempted_at = Column(DateTime, nullable=True)
    source = relationship('Bodega', foreign_keys=[source_id])
    destination = relationship('Bodega', foreign_keys=[destination_id])
    items = relationship('InventoryRequestItem', back_populates='request', cascade='all, delete-orphan', order_by='InventoryRequestItem.id')
    events = relationship('InventoryRequestEvent', back_populates='request', cascade='all, delete-orphan', order_by='InventoryRequestEvent.id')

    @property
    def numero(self):
        return f'REQ-{self.id:06d}'

    @property
    def status_label(self):
        return {'pending': 'Pendiente de autorización', 'active': 'Activa · traslado realizado', 'cancelled': 'Anulada'}[self.status]


class InventoryRequestItem(Base):
    __tablename__ = 'inventory_request_items'
    __table_args__ = (CheckConstraint('quantity > 0 AND quantity <= 1000000', name='ck_inventory_request_quantity'), UniqueConstraint('request_id','producto_id',name='uq_inventory_request_product'))
    id = Column(Integer, primary_key=True)
    request_id = Column(Integer, ForeignKey('inventory_requests.id'), nullable=False, index=True)
    producto_id = Column(Integer, ForeignKey('productos.id'), nullable=False)
    code = Column(String(60), nullable=False)
    description = Column(String(200), nullable=False)
    quantity = Column(Integer, nullable=False)
    cost_cs = Column(Numeric(14,2), nullable=True)
    request = relationship('InventoryRequest', back_populates='items')
    product = relationship('Producto')


class InventoryRequestEvent(Base):
    __tablename__ = 'inventory_request_events'
    id = Column(Integer, primary_key=True)
    request_id = Column(Integer, ForeignKey('inventory_requests.id'), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    user_name = Column(String(160), nullable=False)
    action = Column(String(40), nullable=False)
    detail = Column(Text, nullable=False, default='')
    created_at = Column(DateTime, nullable=False)
    request = relationship('InventoryRequest', back_populates='events')
