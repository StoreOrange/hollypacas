"""Duplicate confirmations use one correction and one delta in an isolated local schema."""
import sys,uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch
from datetime import date
from sqlalchemy import create_engine,text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.config import get_active_database_url
from tests.test_payroll_start_date import StartDateTests
from app.routers import web as w
from app.core import payroll_start_date as service
from app.models.payroll import PayrollStartDateChange

url=get_active_database_url()
if make_url(url).host not in {'localhost','127.0.0.1','::1'}:raise SystemExit('Local PostgreSQL only')
control=create_engine(url,echo=False);schema='qa_hire_date_'+uuid.uuid4().hex
with control.begin() as conn:conn.execute(text(f'CREATE SCHEMA {schema}'))
engine=create_engine(url,echo=False,connect_args={'options':f'-csearch_path={schema} -clock_timeout=10000 -cstatement_timeout=20000'})
case=StartDateTests()
try:
    with patch('sqlalchemy.create_engine',return_value=engine):case.setUp()
    case.accrue();data=case.reviewed()
    before=case.db.query(w.AccountingEntry).count();ident=case.profile.employee_id
    case.db.rollback()  # End the read-only review transaction and release its accounting-date lock.
    barrier=Barrier(2)
    def apply():
        with Session(engine) as db:
            user=db.get(w.User,1)
            barrier.wait(timeout=10)
            audit=service.apply(db,user,ident,date.fromisoformat(data['new_date']),data['reason'],data['nonce'],data['fingerprint'],data['signature'])
            db.commit();return audit.id
    with ThreadPoolExecutor(max_workers=2) as workers:
        results=[workers.submit(apply) for _ in range(2)]
        ids=[f.result(timeout=30) for f in results]
    with Session(engine) as db:
        assert ids[0]==ids[1]
        assert db.query(PayrollStartDateChange).count()==1
        assert db.query(w.AccountingEntry).count()==before+1
    print('PASS PostgreSQL simultaneous confirmations: one audit, one accounting adjustment')
finally:
    if hasattr(case,'client'):case.tearDown()
    engine.dispose()
    with control.begin() as conn:conn.execute(text(f'DROP SCHEMA {schema} CASCADE'))
    control.dispose()
