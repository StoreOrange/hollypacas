"""Manual localhost-only regression: real HTTP handlers, all financial writes rolled back.

Run from backend. Never connects to a non-local database or runs application startup.
"""
import sys
import logging
from pathlib import Path
from decimal import Decimal
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sqlalchemy import create_engine,text,inspect
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from fastapi.testclient import TestClient
from app.config import get_active_database_url,get_active_company_key
from app.main import app
from app.routers import web as w
from app.core.mobile_expenses import payment_choices
from app.core.mobile_expense_schema import ensure_mobile_expense_columns
from app.core.security import create_access_token
from app.models.sales import MobileExpense


def main():
    url=get_active_database_url()
    assert make_url(url).host in {'127.0.0.1','localhost'} and get_active_company_key()=='hollywood_pacas'
    engine=create_engine(url)
    logging.getLogger('sqlalchemy.engine.Engine').disabled=True
    # Private temp tables shadow real names on this connection only.
    with engine.connect() as conn:
        tx=conn.begin()
        try:
            conn.execute(text('CREATE TEMP TABLE accounting_entries (id int,moneda varchar(10),total_debe numeric,tasa_cambio numeric,tasa_sistema numeric) ON COMMIT DROP'))
            conn.execute(text('CREATE TEMP TABLE recibos_caja (id int,monto_usd numeric,tasa_cambio numeric) ON COMMIT DROP'))
            conn.execute(text('CREATE TEMP TABLE mobile_expenses (id int,entry_id int,cash_receipt_id int) ON COMMIT DROP'))
            conn.execute(text("INSERT INTO accounting_entries VALUES (1,'CS',100,NULL,37),(2,'USD',3700,37,37),(3,'USD',45678.72,37,37)"))
            conn.execute(text('INSERT INTO recibos_caja VALUES (7,1234.56,37)'))
            conn.execute(text('INSERT INTO mobile_expenses VALUES (1,1,NULL),(2,2,NULL),(3,3,7)'))
            assert set(ensure_mobile_expense_columns(conn))=={'amount','currency','exchange_rate'}
            rows=conn.execute(text('SELECT amount,currency,exchange_rate FROM mobile_expenses ORDER BY id')).all()
            assert [(row[0],row[1]) for row in rows]==[(Decimal('100'),'CS'),(Decimal('100'),'USD'),(Decimal('1234.56'),'USD')]
            assert all(row[2]==37 for row in rows)
            assert ensure_mobile_expense_columns(conn)==[]
            conn.execute(text('ALTER TABLE mobile_expenses DROP COLUMN amount'))
            conn.execute(text('DELETE FROM accounting_entries WHERE id=1'))
            try:
                with conn.begin_nested():ensure_mobile_expense_columns(conn)
            except RuntimeError:pass
            else:raise AssertionError('Invalid legacy data should block the migration')
            assert 'amount' not in {c['name'] for c in inspect(conn).get_columns('mobile_expenses')}
            print('PASS: migración PostgreSQL de CS/USD/ROC, idempotencia y rollback ante soporte inválido; solo tablas temporales privadas.')
        finally:tx.rollback()
    # Real local configuration, real handlers and committed savepoints; outer
    # transaction is rolled back even on errors. No test expense persists.
    with engine.connect() as conn:
        tx=conn.begin();db=Session(bind=conn,join_transaction_mode='create_savepoint')
        before={table:conn.execute(text('SELECT COUNT(*) FROM '+table)).scalar_one() for table in ('mobile_expenses','accounting_entries','recibos_caja')}
        original=dict(app.dependency_overrides)
        try:
            user=db.query(w.User).filter(w.User.is_active.is_(True),w.User.roles.any(w.Role.name=='administrador')).order_by(w.User.id).first()
            assert user
            token=create_access_token({'sub':user.email})
            def local_db():yield db
            app.dependency_overrides[w.get_db]=local_db
            client=TestClient(app,raise_server_exceptions=True)
            client.cookies.set('access_token',token)
            page=client.get('/m/gastos');assert page.status_code==200,page.status_code
            assert 'id="mobileExpenseForm"' in page.text
            scopes=w._user_scoped_branch_ids(db,user);day=w.local_today()
            warehouse=db.query(w.Bodega).filter(w.Bodega.activo.is_(True),w.Bodega.branch_id.in_(scopes)).order_by(w.Bodega.id).first()
            assert warehouse
            cash=next(c for c in payment_choices(db,{warehouse.branch_id}) if c['kind']=='cash')
            key='6db8ceeed9184f889ae849065e27009a'
            data={'operation_key':key,'branch_id':str(warehouse.branch_id),'bodega_id':str(warehouse.id),'fecha':day.isoformat(),
                  'category':'extraordinary','descripcion':'Verificación local transaccional; NO guardar','beneficiary':'PRUEBA LOCAL CON ROLLBACK',
                  'reference':'QA-LOCAL-ROLLBACK','payment':cash['value'],'moneda':'CS','monto':'1.00'}
            response=client.post('/m/gastos',data=data,follow_redirects=False)
            assert response.status_code==303,(response.status_code,response.text[-1500:])
            saved=db.query(MobileExpense).filter_by(operation_key=key).one()
            assert saved.cash_receipt_id and saved.entry.total_debe==1
            repeat=client.post('/m/gastos',data=data,follow_redirects=False)
            assert repeat.headers['location']==response.headers['location']
            assert db.query(MobileExpense).filter_by(operation_key=key).count()==1
            pdf=client.get(f'/m/gastos/{saved.id}/pdf');assert pdf.status_code==200 and pdf.content.startswith(b'%PDF')
            try:
                import fitz
                document=fitz.open(stream=pdf.content,filetype='pdf')
                assert 'RECIBO DE GASTO' in ''.join(p.get_text() for p in document)
            except ImportError:pass
            print('PASS: página real, caja configurada con subcuenta, registro HTTP, reintento sin duplicado y PDF; todo dentro de una transacción reversible.')
            client.close()
        finally:
            app.dependency_overrides.clear();app.dependency_overrides.update(original)
            db.close();tx.rollback()
        after={table:conn.execute(text('SELECT COUNT(*) FROM '+table)).scalar_one() for table in before}
        assert after==before,(before,after)
        print('PASS: rollback confirmado; no quedó ningún gasto, asiento ni ROC de prueba en la base local.')


if __name__=='__main__':main()
