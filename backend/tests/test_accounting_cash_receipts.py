import unittest
from decimal import Decimal
from app.core.accounting_cash_receipts import receipt_amounts

class CashReceiptAmountsTests(unittest.TestCase):
    def test_cordobas(self):
        self.assertEqual(receipt_amounts('1,000.00', 'CS', '36.6243'), (Decimal('27.30'), Decimal('1000.00')))
    def test_dollars(self):
        self.assertEqual(receipt_amounts('100.00', 'USD', '36.6243'), (Decimal('100.00'), Decimal('3662.43')))
    def test_invalid_amounts(self):
        for value in ['NaN', 'Infinity', '-1', '0', '100.001', '1,2', '1.000,25']:
            with self.subTest(value=value), self.assertRaises(ValueError): receipt_amounts(value, 'CS', '36')
    def test_invalid_exchange(self):
        for value in ['NaN', '-1', '0']:
            with self.subTest(value=value), self.assertRaises(ValueError): receipt_amounts('100','USD',value)
    def test_cordobas_without_exchange(self):
        self.assertEqual(receipt_amounts('100','CS','0'),(Decimal(0),Decimal(100)))
    def test_unsupported_currency(self):
        with self.assertRaises(ValueError): receipt_amounts('100','EUR','36')
