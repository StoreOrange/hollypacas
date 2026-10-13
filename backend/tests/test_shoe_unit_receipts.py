import unittest
from decimal import Decimal
from app.routers.shoe_unit_receipts import validate_rows

class UnitReceiptValidationTests(unittest.TestCase):
    def row(self, **changes):
        row = dict(code='ABC-01', name='Zapato', line_id=1, brand='Marca', color='Negro', size='38', qty=1, cost='100.00', price='200.00')
        row.update(changes)
        return row

    def test_one_unit(self):
        row = validate_rows([self.row()])[0]
        self.assertEqual(row['qty'], Decimal(1))
        self.assertEqual(row['price'], Decimal('200'))

    def test_multiple_independent_models_and_sizes(self):
        self.assertEqual(len(validate_rows([self.row(), self.row(size='39'), self.row(code='OTHER', price='300')])), 3)

    def test_nonfinite_money(self):
        for value in ['NaN', 'Infinity', '-Infinity']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_rows([self.row(price=value)])

    def test_fractional_or_nonpositive_units(self):
        for value in ['1.5', '0', '-1', '10001', 'NaN']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_rows([self.row(qty=value)])

    def test_two_decimals(self):
        with self.assertRaises(ValueError):
            validate_rows([self.row(price='200.001')])

    def test_cost_less_than_price(self):
        with self.assertRaises(ValueError):
            validate_rows([self.row(cost='200')])

    def test_duplicate_variant(self):
        with self.assertRaises(ValueError):
            validate_rows([self.row(), self.row(color='negro')])

    def test_shared_model_pricing(self):
        with self.assertRaises(ValueError):
            validate_rows([self.row(), self.row(size='39', price='201')])

    def test_missing_size_or_color(self):
        for changes in [dict(size=''), dict(color='')]:
            with self.assertRaises(ValueError):
                validate_rows([self.row(**changes)])

    def test_code_lengths_and_symbols(self):
        for changes in [dict(code='A'*61), dict(scan_code='A'*81), dict(code='<script>')]:
            with self.assertRaises(ValueError):
                validate_rows([self.row(**changes)])

    def test_empty_or_invalid_batch(self):
        for rows in [[], None, {}, [None], [self.row()]*101]:
            with self.assertRaises(ValueError):
                validate_rows(rows)

    def test_normalizes_code_preserves_size(self):
        row = validate_rows([self.row(code=' ab-c ', size=' 38.5 ')])[0]
        self.assertEqual(row['code'], 'AB-C')
        self.assertEqual(row['size'], '38.5')

if __name__ == '__main__':
    unittest.main()
