import asyncio
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode
from starlette.requests import Request
from fastapi import HTTPException
from sqlalchemy import event
from tests import test_accounting_workflow as fixtures
from app.routers import web as w, mobile_expenses as routes
from app.core.mobile_expenses import register_expense, payment_choices, PERMISSION
from app.models.sales import MobileExpense
from app.models.user import Permission
from app.main import app
REAL_ENFORCE=w._enforce_permission


class MobileExpenseTests(unittest.TestCase):
    def setUp(self):
        fixtures.AccountingWorkflowTests.setUp(self)
        self.user.full_name='Gerencia';self.user.hashed_password='unused-test-hash';self.db.add(self.user)
        self.db.add_all([
            w.Bodega(id=1,code='CENTRAL',name='Caja Central',branch_id=1,activo=True),
            w.Bodega(id=2,code='ESTELI',name='Caja Estelí',branch_id=2,activo=True),
            w.CuentaContable(id=3,codigo='6105',nombre='Mantenimiento y Reparaciones',tipo='RESULTADO',naturaleza='DEBE',activo=True),
            w.CuentaContable(id=4,codigo='6901',nombre='Otros Gastos',tipo='RESULTADO',naturaleza='DEBE',activo=True),
            w.CuentaContable(id=5,codigo='1102',nombre='Bancos',tipo='BALANCE',naturaleza='DEBE',activo=True),
            w.CuentaContable(id=6,codigo='2109',nombre='Gastos Acumulados por Pagar',tipo='BALANCE',naturaleza='HABER',activo=True),
            w.CuentaContable(id=7,codigo='1104',nombre='Inventarios',tipo='BALANCE',naturaleza='DEBE',activo=True),
            w.AccountingVoucherType(id=2,code='EGRESO',nombre='Egreso',prefijo='CE',activo=True),
            w.AccountingJournal(id=1,code='CAJA',name='Caja',kind='CASH',currency='CS',voucher_type_id=2,main_account_id=1,active=True),
            w.AccountingJournal(id=2,code='BANCO',name='Banco',kind='BANK',currency='CS',voucher_type_id=2,main_account_id=5,active=True),
            w.AccountingJournal(id=3,code='VAR',name='General',kind='GENERAL',currency='CS',voucher_type_id=1,active=True),
            w.ExchangeRate(effective_date=date(2026,10,1),period="2026-10",rate=37),
            w.CajaDiaria(branch_id=1,bodega_id=1,fecha=self.day,saldo_cs=1000,saldo_usd=100),
        ]);self.db.commit()
        self.patches.extend([patch.object(w,'local_today',return_value=self.day),patch.object(w,'_is_pacasholl_company',return_value=True),patch.object(w,'get_active_company_key',return_value='hollywood_pacas')])
        for p in self.patches[-3:]:p.start()
        self.user.default_branch_id=1;self.user.default_bodega_id=1;self.db.commit()
    def tearDown(self):fixtures.AccountingWorkflowTests.tearDown(self)
    def data(self,**changes):
        data={'operation_key':'a'*32,'branch_id':'1','bodega_id':'1','fecha':self.day.isoformat(),'category':'repair',
              'descripcion':'Reparación urgente de puerta','beneficiary':'Taller de prueba','reference':'FAC-123','payment':'cash:1','moneda':'CS','monto':'100','paid_by':'Propietario'}
        data.update(changes);return data
    def save(self,**changes):
        try:
            result=register_expense(self.db,self.user,self.data(**changes));self.db.commit();return result
        except Exception:
            self.db.rollback();raise
    def req(self,query='',data=None):
        scope=dict(type='http',method='POST' if data else 'GET',path='/m/gastos',scheme='https',server=('test',443),
                   headers=[(b'content-type',b'application/x-www-form-urlencoded')],query_string=query.encode(),app=app,router=app.router)
        async def receive():return {'type':'http.request','body':urlencode(data).encode(),'more_body':False}
        request=Request(scope,receive=receive if data else None)
        request.state.branding={'company_key':'hollywood_pacas'}
        w._set_request_permissions(request,self.user)
        return request
    def test_cash_records_one_receipt_entry_and_cash_movement(self):
        row=self.save()
        self.assertIsNotNone(row.cash_receipt_id)
        self.assertEqual(row.entry.estado,'POSTEADO')
        self.assertEqual(row.entry.total_debe,100)
        self.assertEqual({line.cuenta.codigo for line in row.entry.lines},{'6105','1101'})
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,900)
        self.assertEqual(row.cash_receipt.monto_cs,100)
        self.assertEqual(row.entry.manual_operation_key,'a'*32)
        self.assertIn('FAC-123',row.entry.descripcion)
        self.assertEqual(row.cash_receipt.accounting_entry_id,row.entry_id)
        self.assertTrue(row.numero.startswith('GM-202610-'))
    def test_configured_cash_account_with_children_still_available(self):
        self.db.add(w.CuentaContable(codigo='110101',nombre='Subcaja',parent_id=1,tipo='BALANCE',naturaleza='DEBE',activo=True));self.db.commit()
        self.assertIn('cash:1',{choice['value'] for choice in payment_choices(self.db,{1})})
        row=self.save()
        self.assertEqual(row.entry.lines[1].cuenta_id,1)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,900)

    def test_retry_has_no_second_impact(self):
        first=self.save();second=self.save()
        self.assertEqual(first.id,second.id)
        for model in (MobileExpense,w.ReciboCaja,w.AccountingEntry):self.assertEqual(self.db.query(model).count(),1)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,900)
    def test_bank_only_posts_bank_not_pos_cash(self):
        row=self.save(payment='bank:2')
        self.assertIsNone(row.cash_receipt_id)
        self.assertEqual({line.cuenta.codigo for line in row.entry.lines},{'6105','1102'})
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,1000)
        self.assertEqual(self.db.query(w.ReciboCaja).count(),0)
    def test_personal_creates_reimbursement_not_cash_or_bank(self):
        row=self.save(payment='personal')
        self.assertEqual(row.paid_by,'Propietario')
        self.assertEqual({line.cuenta.codigo for line in row.entry.lines},{'6105','2109'})
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,1000)
    def test_dollars_use_historical_rate_without_float_rounding(self):
        self.db.add(w.ExchangeRate(effective_date=date(2026,10,13),period="2026-10-next",rate=50));self.db.commit()
        row=self.save(moneda='USD',monto='10.25')
        self.assertEqual(row.entry.total_debe,Decimal('379.25'))
        self.assertEqual(row.entry.moneda,'USD');self.assertEqual(row.entry.tasa_cambio,37)
        self.assertEqual(row.amount,Decimal('10.25'));self.assertEqual(row.currency,'USD')
        self.assertEqual(row.cash_receipt.monto_usd,Decimal('10.25'))
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,Decimal('620.75'))
    def test_permission_and_other_company_blocked(self):
        user=w.User(email='vendedor@test',roles=[w.Role(name='vendedor')])
        request=self.req();request.scope['headers'].append((b'accept',b'application/json'))
        with patch.object(w,'_enforce_permission',REAL_ENFORCE),patch.object(w,'_send_permission_sms_alert'):
            with self.assertRaises(HTTPException) as cm:routes.page(request,self.db,user)
            self.assertEqual(cm.exception.status_code,403)
        with patch.object(w,'_is_pacasholl_company',return_value=False):
            with self.assertRaises(HTTPException) as cm:routes.page(self.req(),self.db,self.user)
            self.assertEqual(cm.exception.status_code,404)
    def test_forged_category_account_scope_or_payment_rejected(self):
        for changes in [{'category':'inventory'},{'branch_id':'2'},{'bodega_id':'2'},{'payment':'bank:1'},{'payment':'cash:999'},
                        {'fecha':'2026-10-13'},{'fecha':'bad'},{'beneficiary':''},{'reference':'','no_support':''},{'paid_by':'','payment':'personal'}]:
            with self.subTest(changes=changes),self.assertRaises((ValueError,HTTPException)):self.save(**changes)
        self.assertEqual(self.db.query(MobileExpense).count(),0)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,1000)
        row=self.save(counter_account_id='7',line_cuenta_id='7')
        self.assertEqual(row.entry.lines[0].cuenta.codigo,'6105')
    def test_invalid_money_rolls_back_everything(self):
        for amount in ('0','-1','NaN','Infinity','100.001','1,2','99999999999'):
            with self.subTest(amount=amount),self.assertRaises(ValueError):self.save(monto=amount)
        self.assertEqual(self.db.query(w.ReciboCaja).count(),0)
        self.assertEqual(self.db.query(w.AccountingEntry).count(),0)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,1000)
    def test_closed_cash_and_period_block_registration(self):
        self.db.add(w.CierreCaja(bodega_id=1,branch_id=1,fecha=self.day,usuario_registro='test'));self.db.commit()
        with self.assertRaises(ValueError):self.save()
        with patch.object(w,'_assert_accounting_date_open',side_effect=HTTPException(409,'Cerrado')):
            with self.assertRaises(HTTPException):self.save(payment='bank:2')
        self.assertEqual(self.db.query(MobileExpense).count(),0)
    def test_missing_or_parent_expense_account_never_falls_back_to_inventory(self):
        account=self.db.get(w.CuentaContable,3);account.activo=False;self.db.commit()
        with self.assertRaises(ValueError):self.save()
        account.activo=True;self.db.add(w.CuentaContable(codigo='610501',nombre='Hija',parent_id=3,tipo='RESULTADO',naturaleza='DEBE',activo=True));self.db.commit()
        with self.assertRaises(ValueError):self.save()
    def test_failed_final_insert_rolls_back_receipt_entry_and_cash(self):
        flush=self.db.flush
        def fail(*args,**kwargs):
            if any(isinstance(row,MobileExpense) for row in self.db.new):raise RuntimeError('Simulated expense insert failure')
            return flush(*args,**kwargs)
        with patch.object(self.db,'flush',side_effect=fail),self.assertRaises(RuntimeError):self.save()
        self.assertEqual(self.db.query(w.AccountingEntry).count(),0)
        self.assertEqual(self.db.query(w.ReciboCaja).count(),0)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,1000)
    def test_support_pending_preserved_and_receipt_pdf_scoped(self):
        row=self.save(reference='',no_support='on',payment='personal')
        response=routes.pdf(self.req(),row.id,self.db,self.user)
        async def collect():return b''.join([chunk async for chunk in response.body_iterator])
        body=asyncio.run(collect());self.assertTrue(body.startswith(b'%PDF'))
        try:import fitz
        except ImportError:return
        pdf=fitz.open(stream=body,filetype='pdf');text=''.join(page.get_text() for page in pdf)
        for word in ('RECIBO DE GASTO',row.numero,'Taller de prueba','Propietario','soporte pendiente','2109'):self.assertIn(word,text)
        self.assertEqual(len(pdf),1)
        pdf[0].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).save('/tmp/holl-mobile-expense-receipt.png')
        with patch.object(w,'_user_scoped_branch_ids',return_value={2}):
            with self.assertRaises(HTTPException):routes.pdf(self.req(),row.id,self.db,self.user)
    def test_render_errors_preserve_form_and_no_extra_cash_impact(self):
        data=self.data(monto='NaN')
        response=asyncio.run(routes.create(self.req(data=data),self.db,self.user))
        self.assertEqual(response.status_code,400)
        html=response.body.decode();self.assertIn('Reparación urgente de puerta',html);self.assertIn('value="'+'a'*32+'"',html)
        self.assertEqual(self.db.query(MobileExpense).count(),0)
    def test_cash_expense_cannot_be_deleted_through_old_roc_route(self):
        row=self.save()
        result=w.sales_roc_anular(self.req(),row.cash_receipt_id,self.db,self.user)
        self.assertIn('error=',result.headers['location'])
        self.assertEqual(self.db.query(w.ReciboCaja).count(),1)
        self.assertEqual(self.db.query(w.CajaDiaria).one().saldo_cs,900)
    def test_retry_key_cannot_be_used_by_another_user(self):
        self.save()
        other=w.User(id=999,email='other@test',roles=[w.Role(name='administrador')])
        with self.assertRaises(ValueError):register_expense(self.db,other,self.data())
        self.assertEqual(self.db.query(MobileExpense).count(),1)

    def test_writes_do_not_touch_inventory_or_product_costs(self):
        writes=[]
        def capture(conn,cursor,statement,params,context,many):
            if statement.lstrip().upper().startswith(('INSERT ','UPDATE ','DELETE ')):writes.append(statement.lower())
        event.listen(self.engine,'before_cursor_execute',capture);self.save()
        allowed=('mobile_expenses','accounting_entries','accounting_entry_lines','recibos_caja','recibos_rubros','recibos_motivos','caja_diaria')
        for statement in writes:self.assertTrue(any(table in statement for table in allowed),statement)
