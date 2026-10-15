from datetime import datetime
from sqlalchemy import Column,Integer,String,Boolean,DateTime,ForeignKey,Text
from ..database import Base


class PosAutoStation(Base):
    __tablename__='pos_auto_stations'
    id=Column(String(32),primary_key=True)
    user_id=Column(Integer,ForeignKey('users.id'),nullable=False)
    branch_id=Column(Integer,ForeignKey('branches.id'),nullable=False)
    bodega_id=Column(Integer,ForeignKey('bodegas.id'),nullable=False,index=True)
    token_hash=Column(String(64),nullable=True)
    enabled=Column(Boolean,nullable=False,default=False)
    baseline_id=Column(Integer,nullable=False,default=0)
    enabled_at=Column(DateTime,nullable=True)
    last_tick=Column(DateTime,nullable=True)
    agent_seen=Column(DateTime,nullable=True)
    agent_ready=Column(Boolean,nullable=False,default=False)
    printer_name=Column(String(200),nullable=True)
    agent_message=Column(String(300),nullable=True)


class PosAutoTask(Base):
    __tablename__='pos_auto_tasks'
    id=Column(Integer,primary_key=True)
    station_id=Column(String(32),ForeignKey('pos_auto_stations.id'),nullable=False,index=True)
    preventa_id=Column(Integer,ForeignKey('ventas_preventas.id'),nullable=False,unique=True)
    operation_key=Column(String(32),nullable=False,unique=True)
    due_at=Column(DateTime,nullable=False)
    status=Column(String(20),nullable=False,default='WAITING')
    invoice_id=Column(Integer,ForeignKey('ventas_facturas.id'),nullable=True)
    message=Column(String(400),nullable=True)


class PosPrintJob(Base):
    __tablename__='pos_print_jobs'
    id=Column(Integer,primary_key=True)
    station_id=Column(String(32),ForeignKey('pos_auto_stations.id'),nullable=False,index=True)
    invoice_id=Column(Integer,ForeignKey('ventas_facturas.id'),nullable=False,unique=True)
    status=Column(String(20),nullable=False,default='QUEUED')
    claim_id=Column(String(32),nullable=True)
    copies=Column(Integer,nullable=False,default=1)
    message=Column(String(400),nullable=True)
    created_at=Column(DateTime,nullable=False,default=datetime.utcnow)
    dispatched_at=Column(DateTime,nullable=True)
    completed_at=Column(DateTime,nullable=True)
