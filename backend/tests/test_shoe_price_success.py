"""Authoritative success details for prices; all writes are isolated SQLite only."""
import json,unittest
from datetime import date
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from app.database import Base
from app.routers import web
from app.models.user import User,Role,Branch
from app.models.inventory import Producto,ColorCatalog,ShoeProductVariant,ShoeVariantStock,Bodega,ExchangeRate

class ShoePriceSuccessTests(unittest.TestCase):
 def test_success_returns_only_updated_products_and_all_variants_without_moving_stock(self):
  engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
  try:
   with Session(engine) as db:
    db.add_all([Branch(id=1,code='central',name='Central'),Bodega(id=1,branch_id=1,code='central',name='Central'),ColorCatalog(id=1,nombre='BLACK PU',abreviatura='BLK'),ColorCatalog(id=2,nombre='IVORY',abreviatura='IVY'),Producto(id=1,cod_producto='EQUAL02',descripcion='Zapato',costo_producto=50,precio_venta1=1000),Producto(id=2,cod_producto='OTHER',descripcion='Otro',precio_venta1=1100),ExchangeRate(effective_date=date(2020,1,1),period='DIA',rate=37)])
    db.add_all([ShoeProductVariant(id=1,producto_id=1,color_id=1,talla='6.5',cod_variante='V1'),ShoeProductVariant(id=2,producto_id=1,color_id=2,talla='8',cod_variante='V2'),ShoeProductVariant(id=3,producto_id=2,color_id=1,talla='7',cod_variante='V3'),ShoeVariantStock(variante_id=1,bodega_id=1,existencia=5)]);db.commit()
    user=User(email='qa@example.invalid',roles=[Role(name='administrador')]);req=Request({'type':'http','method':'POST','path':'/inventory/shoes/prices/apply','headers':[]})
    values={'precio_venta'+str(i):900-(i-1)*10 for i in range(1,8)}
    with patch.object(web,'_is_shoes_mode',return_value=True):
     result=web.inventory_shoes_prices_apply(req,product_ids='1,99999',db=db,user=user,**values)
    payload=json.loads(result.body);self.assertTrue(payload['ok']);self.assertEqual(payload['updated_count'],1)
    self.assertEqual(len(payload['updated_products']),1)
    product=payload['updated_products'][0];self.assertEqual(product['cod_producto'],'EQUAL02')
    self.assertEqual({(v['color'],v['talla']) for v in product['variantes']},{('BLACK PU','6.5'),('IVORY','8')})
    self.assertEqual(db.get(Producto,1).precio_venta1,900);self.assertEqual(db.get(Producto,1).precio_venta7,840)
    self.assertEqual(db.get(Producto,1).costo_producto,50);self.assertEqual(db.get(Producto,2).precio_venta1,1100)
    self.assertEqual(db.query(ShoeVariantStock).one().existencia,5)
    with patch.object(web,'_is_shoes_mode',return_value=True),patch.object(db,'commit',side_effect=RuntimeError('simulated commit failure')):
     with self.assertRaises(RuntimeError):web.inventory_shoes_prices_apply(req,product_ids='1',db=db,user=user,**{key:800 for key in values})
    db.rollback();self.assertEqual(db.get(Producto,1).precio_venta1,900)
  finally:engine.dispose()
