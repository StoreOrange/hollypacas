import unittest
from decimal import Decimal as D
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.core.inventory_report_costs import consolidated_cost
from app.database import Base
from app.models.inventory import Producto, Bodega
from app.models.user import Branch
from app.routers import web


class InventoryReportCostsTest(unittest.TestCase):
    def test_requested_percentages_and_exact_codes(self):
        for code, expected in [('HL31','1030.00'),('DL17','1030.00'),('DL102','1010.00'),
                               ('USM84','1040.00'),('USM80','1030.00'),('HL310','1000.00'),('DL117','1000.00')]:
            with self.subTest(code=code):
                self.assertEqual(consolidated_cost(code,D('100'),D('10'),enabled=True)[0],D(expected))
        self.assertEqual(consolidated_cost(' hl31 ',D('100'),D('10'),enabled=True)[0],D('1030'))

    def test_current_balances_in_requested_increase_range(self):
        stock = [('HL31', '121', '2960'), ('DL17', '108', '5180'),
                 ('DL102', '113', '6290'), ('USM84', '111', '8140'), ('USM80', '91', '12950')]
        increase = sum(consolidated_cost(code, D(cost), D(qty), enabled=True)[0] - D(cost)*D(qty)
                       for code, qty, cost in stock)
        self.assertEqual(increase, D('106130.80'))
        self.assertGreaterEqual(increase, D('100000'))
        self.assertLessEqual(increase, D('113550'))

    def test_no_adjustment_without_stock_or_in_other_company(self):
        for quantity in ['0','-3']:
            self.assertEqual(consolidated_cost('HL31',D('100'),D(quantity),enabled=True),
                             (D('100')*D(quantity),D('0')))
        self.assertEqual(consolidated_cost('HL31',D('100'),D('3'),enabled=False),(D('300'),D('0')))
        self.assertEqual(consolidated_cost('DL102',D('0.50'),D('1'),enabled=True)[0],D('0.51'))

    def test_branch_and_combined_report_preserve_cost_and_reconcile(self):
        engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
        with Session(engine) as db:
            db.add_all([Branch(id=1,code='A',name='A'),Branch(id=2,code='B',name='B'),
                        Bodega(id=1,code='A',name='A',branch_id=1),Bodega(id=2,code='B',name='B',branch_id=2)])
            codes=['HL31','DL17','DL102','USM84','USM80','OTHER','ZERO']
            for i,code in enumerate(codes,1):
                db.add(Producto(id=i,cod_producto=code,descripcion=code,costo_producto=D('100'),activo=True))
            db.commit()
            balances={(i,b):D(str(b+1)) for i in range(1,7) for b in (1,2)}
            with patch.object(web,'_is_pacasholl_company',return_value=True),patch.object(web,'_scoped_branches_query',side_effect=lambda db:db.query(Branch)),patch.object(web,'_scoped_bodegas_query',side_effect=lambda db:db.query(Bodega)),patch.object(web,'_balances_by_bodega',return_value=balances):
                totals=[]
                for branch,qty in [('1',D('2')),('2',D('3')),('all',D('5'))]:
                    rows,total_qty,total_cost,*_=web._inventory_consolidated_data(db,branch)
                    self.assertEqual(len(rows),6);self.assertEqual(total_qty,qty*6)
                    self.assertEqual(total_cost,sum(r['costo_total'] for r in rows))
                    self.assertEqual(total_cost,qty*D('614'))
                    self.assertTrue(all(r['costo_unitario']==D('100') for r in rows))
                    totals.append(total_cost)
                self.assertEqual(totals[2],totals[0]+totals[1])
                self.assertFalse(db.dirty)
                self.assertTrue(all(p.costo_producto==D('100') for p in db.query(Producto).all()))
        engine.dispose()
