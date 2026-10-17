from sqlalchemy import Column,Integer,String,Date,DateTime,ForeignKey,Numeric,CheckConstraint,Index,LargeBinary,UniqueConstraint
from sqlalchemy.orm import relationship,deferred
from ..database import Base


class MotoTrip(Base):
    __tablename__='moto_trips'
    __table_args__=(CheckConstraint('amount_cs > 0',name='ck_moto_amount'),CheckConstraint("status IN ('PENDIENTE','ENTREGADO','RECIBIDO','ANULADO')",name='ck_moto_status'),Index('ix_moto_day_seller','trip_date','seller_id'),Index('ix_moto_rider_day','rider_id','trip_date'))
    id=Column(Integer,primary_key=True)
    operation_key=Column(String(32),nullable=False,unique=True)
    rider_id=Column(Integer,ForeignKey('users.id'),nullable=False)
    seller_id=Column(Integer,ForeignKey('vendedores.id'),nullable=False)
    branch_id=Column(Integer,ForeignKey('branches.id'),nullable=False)
    bodega_id=Column(Integer,ForeignKey('bodegas.id'),nullable=False)
    trip_date=Column(Date,nullable=False)
    destination=Column(String(160),nullable=False)
    amount_cs=Column(Numeric(14,2),nullable=False)
    comments=Column(String(500),nullable=False,default='')
    status=Column(String(20),nullable=False,default='PENDIENTE')
    receipt_id=Column(Integer,ForeignKey('recibos_caja.id'),nullable=True,unique=True)
    reversal_receipt_id=Column(Integer,ForeignKey('recibos_caja.id'),nullable=True,unique=True)
    accounting_entry_id=Column(Integer,ForeignKey('accounting_entries.id'),nullable=True,unique=True)
    reversal_entry_id=Column(Integer,ForeignKey('accounting_entries.id'),nullable=True,unique=True)
    closure_id=Column(Integer,ForeignKey('moto_trip_closures.id'),nullable=True,index=True)
    closure=relationship('MotoTripClosure',back_populates='trips')
    created_at=Column(DateTime,nullable=False)
    accounting_entry=relationship('AccountingEntry',foreign_keys=[accounting_entry_id])
    reversal_entry=relationship('AccountingEntry',foreign_keys=[reversal_entry_id])
    photos=relationship('MotoTripPhoto',order_by='MotoTripPhoto.id')
    rider=relationship('User',foreign_keys=[rider_id])
    seller=relationship('Vendedor')
    receipt=relationship('ReciboCaja',foreign_keys=[receipt_id])
    reversal_receipt=relationship('ReciboCaja',foreign_keys=[reversal_receipt_id])
    events=relationship('MotoTripEvent',back_populates='trip',order_by='MotoTripEvent.id')


class MotoTripEvent(Base):
    __tablename__='moto_trip_events'
    id=Column(Integer,primary_key=True)
    trip_id=Column(Integer,ForeignKey('moto_trips.id'),nullable=False,index=True)
    actor_id=Column(Integer,ForeignKey('users.id'),nullable=False)
    action=Column(String(20),nullable=False)
    comment=Column(String(500),nullable=False,default='')
    created_at=Column(DateTime,nullable=False)
    trip=relationship('MotoTrip',back_populates='events')
    actor=relationship('User')


class MotoTripPhoto(Base):
    __tablename__='moto_trip_photos'
    __table_args__=(UniqueConstraint('trip_id','digest',name='uq_moto_photo_content'),)
    id=Column(Integer,primary_key=True)
    trip_id=Column(Integer,ForeignKey('moto_trips.id'),nullable=False,index=True)
    uploaded_by_id=Column(Integer,ForeignKey('users.id'),nullable=False)
    original_name=Column(String(180),nullable=False)
    digest=Column(String(64),nullable=False)
    data=deferred(Column(LargeBinary,nullable=False))
    created_at=Column(DateTime,nullable=False)


class MotoTripClosure(Base):
    __tablename__='moto_trip_closures'
    __table_args__=(CheckConstraint('amount_cs > 0',name='ck_moto_closure_amount'),Index('ix_moto_closure_rider_date','rider_id','closure_date'))
    id=Column(Integer,primary_key=True)
    operation_key=Column(String(32),nullable=False,unique=True)
    rider_id=Column(Integer,ForeignKey('users.id'),nullable=False)
    seller_id=Column(Integer,ForeignKey('vendedores.id'),nullable=False)
    branch_id=Column(Integer,ForeignKey('branches.id'),nullable=False)
    closed_by_id=Column(Integer,ForeignKey('users.id'),nullable=False)
    closure_date=Column(Date,nullable=False)
    closed_at=Column(DateTime,nullable=False)
    amount_cs=Column(Numeric(14,2),nullable=False)
    trip_count=Column(Integer,nullable=False)
    comment=Column(String(500),nullable=False,default='')
    rider=relationship('User',foreign_keys=[rider_id])
    seller=relationship('Vendedor')
    closed_by=relationship('User',foreign_keys=[closed_by_id])
    trips=relationship('MotoTrip',back_populates='closure',order_by='MotoTrip.id')
