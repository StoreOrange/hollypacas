import json
import unittest
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.database import Base
from app.routers import web
from app.models.inventory import Producto, ColorCatalog, ShoeProductVariant, ShoeVariantStock, Bodega
from app.models.user import Branch

class QuickTransferExactSearchTest(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine)
        self.db=Session(self.engine)
        self.db.add_all([Branch(id=1,code='central',name='Central'),Bodega(id=1,code='A',name='A',branch_id=1),
            Producto(id=1,cod_producto='SHOE',descripcion='Zapato'),ColorCatalog(id=1,nombre='Negro',abreviatura='N'),
            ShoeProductVariant(id=1,producto_id=1,color_id=1,talla='38',cod_variante='SHOE-38'),
            ShoeProductVariant(id=2,producto_id=1,color_id=1,talla='39',cod_variante='SHOE-39'),
            ShoeVariantStock(variante_id=1,bodega_id=1,existencia=2)])
        self.db.commit()
    def tearDown(self):
        self.db.close();self.engine.dispose()
    def search(self,q,exact=True,bodega='1'):
        with patch.object(web,'_enforce_permission'),patch.object(web,'_is_shoes_mode',return_value=True),patch.object(web,'_scoped_bodegas_query',side_effect=lambda db:db.query(Bodega)):
            return web.inventory_quick_transfers_search(None,q=q,exact=exact,bodega_id=bodega,db=self.db,user=None)
    def test_exact_does_not_substitute_partial_matches(self):
        for code in ['UNKNOWN','SHOE','SHOE-3','Zapato','Negro','38','V1','%','_']:
            with self.subTest(code=code):
                self.assertEqual(json.loads(self.search(code).body)['items'],[])
        for code in ['SHOE-38','shoe-38',' V000001 ']:
            rows=json.loads(self.search(code).body)['items']
            self.assertEqual([r['variant_id'] for r in rows],[1]);self.assertEqual(rows[0]['scan_code'],'V000001')
    def test_manual_search_and_origin_and_zero_stock(self):
        self.assertEqual(len(json.loads(self.search('SHOE',exact=False).body)['items']),2)
        self.assertEqual(json.loads(self.search('SHOE-39').body)['items'][0]['existencia'],0)
        for origin in ['','999','invalid']:
            self.assertEqual(self.search('V000001',bodega=origin).status_code,400)
