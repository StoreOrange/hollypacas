import unittest
from types import SimpleNamespace
from decimal import Decimal
from app.core.accounting_payroll import decomposition, amount

class PayrollAccountingTests(unittest.TestCase):
    def calc(self,**changes):
        row=dict(gross_pay=10000,total_deductions=1500,net_pay=8500,late_deduction=200,deduction_lines=[SimpleNamespace(amount=500),SimpleNamespace(amount=700)])
        row.update(changes);return SimpleNamespace(**row)
    def test_components(self):
        self.assertEqual(decomposition(self.calc()),(Decimal(10000),Decimal(8500),Decimal(200),Decimal(100)))
    def test_zero_deductions(self):
        self.assertEqual(decomposition(self.calc(total_deductions=0,net_pay=10000,late_deduction=0,deduction_lines=[]))[-1],0)
    def test_unbalanced_net(self):
        with self.assertRaises(ValueError):decomposition(self.calc(net_pay=9999))
    def test_deduction_breakdown_larger_than_total(self):
        with self.assertRaises(ValueError):decomposition(self.calc(total_deductions=1000,net_pay=9000))
    def test_invalid_amounts(self):
        for number in ['NaN','Infinity',-1,'1.001']:
            with self.subTest(number=number),self.assertRaises(ValueError):amount(number)
    def test_zero_pay_is_valid_accrual(self):
        self.assertEqual(decomposition(self.calc(gross_pay=1500,net_pay=0))[1],0)
    def test_negative_pay_requires_correction(self):
        with self.assertRaises(ValueError):decomposition(self.calc(net_pay=-1))
