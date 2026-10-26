import unittest
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy import event
from tests.test_accounting_workflow import AccountingWorkflowTests
from app.routers import web as w


class ProductionPurchaseTests(unittest.TestCase):
    setUp = AccountingWorkflowTests.setUp
    tearDown = AccountingWorkflowTests.tearDown
    manual_create = AccountingWorkflowTests.manual_create

    def test_published_purchase_only_changes_accounting(self):
        production = self.db.get(w.CuentaContable, 1)
        production.codigo = '1112'
        production.nombre = 'Inventario de producción en proceso'
        supplier = self.db.get(w.CuentaContable, 2)
        supplier.codigo = '2102'; supplier.nombre = 'Proveedores'
        supplier.tipo = 'BALANCE'
        self.db.add(w.CuentaContable(codigo='1104', nombre='Inventarios', tipo='BALANCE', naturaleza='DEBE', activo=True))
        self.db.commit()
        writes = []
        def capture(conn, cursor, statement, parameters, context, executemany):
            if statement.lstrip().upper().startswith(('INSERT ', 'UPDATE ', 'DELETE ')):
                writes.append(statement.lower())
        event.listen(self.engine, 'before_cursor_execute', capture)
        with patch.object(w, '_is_pacasholl_company', return_value=True):
            response = self.manual_create(state='POSTEADO', debit='100000', credit='100000', key='b'*32)
        self.assertNotIn('error=', response.headers['location'])
        row = self.db.query(w.AccountingEntry).one()
        self.assertEqual(row.estado, 'POSTEADO')
        self.assertEqual(row.total_debe, Decimal('100000'))
        self.assertEqual({line.cuenta.codigo for line in row.lines}, {'1112', '2102'})
        self.assertTrue(writes)
        for statement in writes:
            self.assertTrue(any(table in statement for table in ('accounting_entries', 'accounting_entry_lines', 'accounting_sequences', 'accounting_journals')), statement)
        self.assertEqual(self.db.query(w.AccountingEntryLine).join(w.CuentaContable).filter(w.CuentaContable.codigo=='1104').count(), 0)
