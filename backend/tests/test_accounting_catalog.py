import unittest
from datetime import date
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.database import Base
from app.routers import web as w
from app.core.accounting_catalog import save_account, chart_rows, account_paths, suggested_code
from app.core.init_db import _seed_cuentas_contables, _seed_accounting_voucher_types


class AccountingCatalogTests(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine)
        self.db.add_all([w.CuentaContable(id=1,codigo='1',nombre='Activo',tipo='BALANCE',naturaleza='DEBE',nivel=1,activo=True),
            w.CuentaContable(id=2,codigo='11',nombre='Corriente',tipo='BALANCE',naturaleza='DEBE',parent_id=1,nivel=2,activo=True),
            w.CuentaContable(id=3,codigo='1101',nombre='Caja',tipo='BALANCE',naturaleza='DEBE',parent_id=2,nivel=3,activo=True)])
        self.db.commit()

    def tearDown(self):self.db.close();self.engine.dispose()

    def save(self,code='110101',parent=3,ident=None,name='Caja sucursal',kind='BALANCE',active=True):
        return save_account(self.db,account_id=ident,codigo=code,nombre=name,descripcion='Uso específico',tipo=kind,naturaleza='DEBE',parent_id=parent,active=active)

    def test_subaccount_location_description_and_code(self):
        accounts=self.db.query(w.CuentaContable).all();parent=self.db.get(w.CuentaContable,3)
        self.assertEqual(suggested_code(parent,accounts),'110101')
        row=self.save();self.db.commit()
        self.assertEqual(row.nivel,4);self.assertEqual(row.parent_id,3);self.assertEqual(row.descripcion,'Uso específico')
        path=account_paths(self.db.query(w.CuentaContable).all())[row.id]
        self.assertIn('1 · Activo › 11 · Corriente › 1101 · Caja',path)
        self.assertEqual(suggested_code(parent,self.db.query(w.CuentaContable).all()),'110102')

    def test_reject_cycles_and_bad_child_codes(self):
        for args in [dict(ident=1,code='1',parent=3),dict(ident=2,code='11',parent=2),dict(code='9901',parent=3)]:
            with self.assertRaises(ValueError):self.save(**args)
        self.assertEqual(self.db.get(w.CuentaContable,1).parent_id,None)

    def test_fifth_level_is_rejected(self):
        row=self.save();self.db.commit()
        with self.assertRaises(ValueError):self.save(code='11010101',parent=row.id)

    def test_parent_type_and_duplicate_code_validation(self):
        with self.assertRaises(ValueError):self.save(kind='RESULTADO')
        with self.assertRaises(ValueError):self.save(code='1101',parent=2)

    def test_move_recalculates_subtree_depth(self):
        child=self.save();self.db.commit()
        self.save(ident=3,code='1101',parent=1,name='Caja reorganizada');self.db.commit()
        self.assertEqual(self.db.get(w.CuentaContable,3).nivel,2);self.assertEqual(child.nivel,3)

    def test_move_cannot_push_descendants_past_four_levels(self):
        self.db.add_all([w.CuentaContable(id=4,codigo='110',nombre='Grupo intermedio',tipo='BALANCE',naturaleza='DEBE',parent_id=2,nivel=3,activo=True),
            w.CuentaContable(id=5,codigo='110101',nombre='Detalle',tipo='BALANCE',naturaleza='DEBE',parent_id=3,nivel=4,activo=True)])
        self.db.commit()
        with self.assertRaises(ValueError):self.save(ident=3,code='1101',parent=4)
        self.assertEqual(self.db.get(w.CuentaContable,3).parent_id,2)

    def test_used_account_can_rename_but_keeps_code_and_type(self):
        self.db.add(w.AccountingVoucherType(id=1,code='DIARIO',nombre='Diario',prefijo='CD'))
        self.db.add(w.AccountingEntry(id=1,tipo_id=1,numero='CD-1',fecha=date(2026,10,12),periodo='2026-10',estado='POSTEADO'))
        self.db.add(w.AccountingEntryLine(entry_id=1,cuenta_id=3,debe=100,haber=0));self.db.commit()
        with self.assertRaises(ValueError):self.save(ident=3,code='1102',parent=2)
        row=self.save(ident=3,code='1101',parent=2,name='Caja mejor descrita');self.db.commit()
        self.assertEqual(row.nombre,'Caja mejor descrita');self.assertEqual(row.descripcion,'Uso específico')

    def test_seed_keeps_user_descriptions_hierarchy_and_voucher_names(self):
        self.db.get(w.CuentaContable,3).parent_id=1;self.db.get(w.CuentaContable,3).nivel=2
        self.db.get(w.CuentaContable,3).nombre='Caja principal personalizada'
        self.db.add(w.AccountingVoucherType(code='DIARIO',nombre='Registro manual personalizado',prefijo='DM',activo=True));self.db.commit()
        _seed_cuentas_contables(self.db);_seed_accounting_voucher_types(self.db)
        self.assertEqual(self.db.get(w.CuentaContable,3).parent_id,1)
        self.assertEqual(self.db.get(w.CuentaContable,3).nombre,'Caja principal personalizada')
        voucher=self.db.query(w.AccountingVoucherType).filter_by(code='DIARIO').one()
        self.assertEqual(voucher.nombre,'Registro manual personalizado');self.assertEqual(voucher.prefijo,'DM')

    def test_tree_is_parent_first(self):
        rows=chart_rows(self.db.query(w.CuentaContable).all())
        self.assertEqual([(r['account'].id,r['depth']) for r in rows],[(1,0),(2,1),(3,2)])
