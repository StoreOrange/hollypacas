"""Concurrency checks in disposable local PostgreSQL schemas, never live records."""
import sys
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch
from sqlalchemy import create_engine,text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.config import get_active_database_url
from app.database import Base
from app.routers import web as w
from app.core import inventory_requests as s
from tests.test_inventory_requests import RequisitionTests

url=get_active_database_url()
if make_url(url).host not in {'localhost','127.0.0.1','::1'}:
    raise SystemExit('Concurrency tests require a local PostgreSQL database')
control=create_engine(url,echo=False)
for duplicate in (True,False):
    schema='qa_requisition_'+uuid.uuid4().hex
    with control.begin() as conn:conn.execute(text(f'CREATE SCHEMA {schema}'))
    engine=create_engine(url,echo=False,connect_args={'options':f'-csearch_path={schema} -clock_timeout=10000 -cstatement_timeout=20000'})
    case=RequisitionTests()
    try:
        with patch('tests.test_inventory_requests.create_engine',return_value=engine):case.setUp()
        # Fixtures assign IDs explicitly; advance only this disposable schema's sequences.
        with engine.begin() as conn:
            for table in Base.metadata.sorted_tables:
                if 'id' not in table.c:continue
                sequence=conn.execute(text("SELECT pg_get_serial_sequence(:table,'id')"),{'table':schema+'.'+table.name}).scalar()
                if sequence:
                    conn.execute(text(f"SELECT setval(:sequence, COALESCE((SELECT MAX(id) FROM {table.name}),0)+1,false)"),{'sequence':sequence})
        quantity=2 if duplicate else 6
        row,_=s.create(case.db,case.cashier,uuid.uuid4().hex,2,2,[{'id':1,'quantity':quantity}],'Concurrency QA','http://qa.invalid')
        case.db.commit();first=row.id
        if duplicate:second=first
        else:
            row,_=s.create(case.db,case.cashier,uuid.uuid4().hex,2,2,[{'id':1,'quantity':quantity}],'Concurrency QA','http://qa.invalid')
            case.db.commit();second=row.id
        barrier=Barrier(2)
        def approve(ident):
            with Session(engine) as db:
                admin=db.get(w.User,1)
                barrier.wait(timeout=10)
                result=s.approve(db,admin,ident);db.commit()
                return result.status
        with ThreadPoolExecutor(max_workers=2) as workers:
            futures=[workers.submit(approve,i) for i in (first,second)]
            statuses=[f.result(timeout=30) for f in futures]
        with Session(engine) as db:
            out_count=db.query(w.EgresoInventario).count()
            stock=w._balances_by_bodega(db,[1,2],[1])
            assert out_count==1,(statuses,out_count)
            assert stock[(1,1)]==10-quantity and stock[(1,2)]==quantity,stock
            assert statuses==['active','active'] if duplicate else sorted(statuses)==['active','cancelled']
        print('PASS PostgreSQL:', 'simultaneous duplicate approval creates one transfer' if duplicate else 'competing requests cannot oversell stock')
    finally:
        if hasattr(case,'client'):case.tearDown()
        engine.dispose()
        with control.begin() as conn:conn.execute(text(f'DROP SCHEMA {schema} CASCADE'))
control.dispose()
