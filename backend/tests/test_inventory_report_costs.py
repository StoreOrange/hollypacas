import unittest
from decimal import Decimal as D
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.core.inventory_report_costs import consolidated_cost, branch_cost_increases
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

    def test_central_additional_increase_on_current_balances(self):
        stock = [('HL31', '100', '2960'), ('DL17', '107', '5180'),
                 ('DL102', '103', '6290'), ('USM84', '106', '8140'), ('USM80', '88', '12950')]
        increase = sum(consolidated_cost(code, D(cost), D(qty), enabled=True)[0] - D(cost)*D(qty)
                       for code, qty, cost in stock)
        self.assertEqual(increase, D('100688.10'))
        items = [{'id': i, 'codigo': code, 'costo_unitario': D(cost), 'cantidad': D(qty)}
                 for i, (code, qty, cost) in enumerate(stock)]
        adjusted = sum(branch_cost_increases('central', items, enabled=True).values())
        self.assertEqual(adjusted - increase, D('36450'))
        self.assertEqual(adjusted, D('137138.10'))
        doubled = [dict(item, cantidad=item['cantidad'] * 2) for item in items]
        self.assertEqual(sum(branch_cost_increases('central', doubled, enabled=True).values()), adjusted * 2)

    def test_no_adjustment_without_stock_or_in_other_company(self):
        for quantity in ['0','-3']:
            self.assertEqual(consolidated_cost('HL31',D('100'),D(quantity),enabled=True),
                             (D('100')*D(quantity),D('0')))
        self.assertEqual(consolidated_cost('HL31',D('100'),D('3'),enabled=False),(D('300'),D('0')))
        self.assertEqual(consolidated_cost('DL102',D('0.50'),D('1'),enabled=True)[0],D('0.51'))

    def test_branch_and_combined_report_preserve_cost_and_reconcile(self):
        engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
        with Session(engine) as db:
            db.add_all([Branch(id=1,code='central',name='Central'),Branch(id=2,code='esteli',name='Esteli'),
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
                    self.assertTrue(all(r['costo_total'] == r['costo_unitario'] * r['cantidad'] for r in rows))
                    expected_increase = {'1': D('38.14'), '2': D('30356'), 'all': D('30394.14')}[branch]
                    self.assertEqual(total_cost - sum(r['costo_total'] for r in rows), expected_increase)
                    self.assertTrue(all(r['costo_unitario']==D('100') for r in rows))
                    totals.append(total_cost)
                self.assertEqual(totals[2],totals[0]+totals[1])
                self.assertFalse(db.dirty)
                self.assertTrue(all(p.costo_producto==D('100') for p in db.query(Producto).all()))
        engine.dispose()


class EsteliAllocationTest(unittest.TestCase):
    def test_target_and_remainder_with_stock_changes(self):
        items = [{'id': i, 'codigo': code, 'cantidad': D(qty), 'costo_unitario': D(cost)}
                 for i, (code, qty, cost) in enumerate([
                     ('HL31', '21', '2960'), ('DL17', '1', '5180'), ('DL102', '10', '6290'),
                     ('USM84', '5', '8140'), ('USM80', '3', '12950'), ('OTHER', '100', '100')])]
        increases = branch_cost_increases('esteli', items, enabled=True)
        self.assertEqual(sum(increases.values()), D('30356'))
        self.assertNotIn(5, increases)
        self.assertTrue(all(v == int(v) for v in increases.values()))
        self.assertEqual(increases, branch_cost_increases('esteli', list(reversed(items)), enabled=True))
        items[0]['cantidad'] = D('0')
        increases = branch_cost_increases('esteli', items, enabled=True)
        self.assertNotIn(0, increases)
        self.assertEqual(sum(increases.values()), D('30356'))
        for item in items:
            item['cantidad'] = D('-1')
        self.assertEqual(branch_cost_increases('esteli', items, enabled=True), {})

    def test_branch_and_company_isolation(self):
        items = [{'id': 1, 'codigo': 'HL31', 'cantidad': D('2'), 'costo_unitario': D('100')}]
        self.assertEqual(branch_cost_increases('central', items, enabled=True), {1: D('8.17')})
        self.assertEqual(branch_cost_increases('esteli', items, enabled=True), {1: D('30356')})
        self.assertEqual(branch_cost_increases('other', items, enabled=True), {})
        self.assertEqual(branch_cost_increases('esteli', items, enabled=False), {})
