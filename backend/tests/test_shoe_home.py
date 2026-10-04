import unittest
from datetime import date,datetime
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.database import Base
from app.routers import web
from app.core.shoe_home import snapshot, inventory
from app.models.user import Branch
from app.models.inventory import Bodega,Producto,ColorCatalog,ShoeProductVariant,ShoeVariantStock
from app.models.sales import VentaFactura,VentaPago,FormaPago,CierreCaja,DepositoCliente

class ShoeHomeTests(unittest.TestCase):
 def setUp(self):
  self.engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(self.engine);self.db=Session(self.engine);self.day=date(2026,10,3)
  for i in range(1,4):
   self.db.add(Branch(id=i,code=str(i),name=f'Tienda {i}'));self.db.add(Bodega(id=i,branch_id=i,code=str(i),name=f'Tienda {i}'))
  self.db.add_all([FormaPago(id=1,nombre='Efectivo'),FormaPago(id=2,nombre='Banco')]);self.db.commit()
  self.stores=self.db.query(Bodega).order_by(Bodega.id).all()
 def tearDown(self):self.db.close();self.engine.dispose()
 def invoice(self,i,warehouse,amount,state='ACTIVA',excluded=False,day=3):
  self.db.add(VentaFactura(id=i,numero=f'T-{i}',bodega_id=warehouse,moneda='CS',total_cs=amount,total_usd=0,estado=state,setato_excluida=excluded,fecha=datetime(2026,10,day,12)))
 def test_zero_stores_date_annulment_and_payment_currency(self):
  self.invoice(1,1,360);self.invoice(2,2,100);self.invoice(3,1,900,'ANULADA');self.invoice(4,1,800,day=2);self.invoice(5,1,700,excluded=True)
  self.db.add_all([VentaPago(factura_id=1,forma_pago_id=1,moneda='USD',monto_original=10,monto_cs=360,monto_usd=10),VentaPago(factura_id=2,forma_pago_id=2,moneda='CS',monto_original=100,monto_cs=100),VentaPago(factura_id=3,forma_pago_id=1,moneda='CS',monto_original=900)])
  self.db.commit();data=snapshot(self.db,self.stores,self.day)
  self.assertEqual(len(data['stores']),3);self.assertEqual(data['totals']['sales'],'460.00')
  self.assertEqual(data['stores'][2]['sales'],'0.00');self.assertEqual(data['totals']['cash_usd'],'10.00');self.assertEqual(data['totals']['cash_cs'],'0.00')
  self.assertEqual(len(data['stores'][1]['movements']),1)
  self.assertEqual(snapshot(self.db,self.stores[1:],self.day)['totals']['sales'],'100.00')
  restricted=snapshot(self.db,self.stores,self.day,sales=False,cash=False)
  self.assertIsNone(restricted['totals']['sales']);self.assertFalse(any(s['movements'] for s in restricted['stores']))
 def test_latest_count_only_and_inventory_scoping(self):
  for i,amount in [(1,100),(2,200)]:
   self.db.add(CierreCaja(id=i,branch_id=1,bodega_id=1,fecha=self.day,total_efectivo_cs=amount,total_efectivo_usd=i,created_at=datetime(2026,10,3,12,i)))
  self.db.add_all([Producto(id=1,cod_producto='MODEL',descripcion='Zapato'),ColorCatalog(id=1,nombre='Negro',abreviatura='N'),ShoeProductVariant(id=1,producto_id=1,color_id=1,talla='38',cod_variante='MODEL-N-38'),ShoeVariantStock(variante_id=1,bodega_id=1,existencia=5)])
  self.db.commit();data=snapshot(self.db,self.stores,self.day)
  self.assertEqual(data['totals']['counted_cs'],'200.00');self.assertEqual(data['totals']['counted_usd'],'2.00')
  self.assertTrue(data['stores'][0]['closed']);self.assertFalse(data['stores'][2]['closed'])
  result=inventory(self.db,self.stores,'V000001');self.assertEqual(len(result),1);self.assertEqual(len(result[0]['stores']),3)
  self.assertEqual(Decimal(result[0]['stores'][0]['qty']),5);self.assertEqual(Decimal(result[0]['stores'][1]['qty']),0)
  self.assertEqual(len(inventory(self.db,self.stores[1:],'MODEL')[0]['stores']),2)
  self.assertEqual(inventory(self.db,self.stores,'DOES-NOT-EXIST'),[]);self.assertEqual(inventory(self.db,self.stores,'%'),[])

 def test_deposits_remain_separate_from_sales_and_cash(self):
  self.invoice(1,1,360)
  self.db.add(DepositoCliente(branch_id=1,bodega_id=1,vendedor_id=1,banco_id=1,fecha=self.day,moneda='USD',monto_usd=25,monto_cs=900,metodo='TRANSFERENCIA'))
  self.db.commit();data=snapshot(self.db,self.stores,self.day)
  self.assertEqual(data['totals']['sales'],'360.00');self.assertEqual(data['totals']['cash_usd'],'0.00')
  movements=data['stores'][0]['movements'];self.assertEqual(len(movements),1);self.assertEqual(movements[0]['amount'],'25.00');self.assertEqual(movements[0]['currency'],'USD')
  self.assertFalse(data['stores'][1]['movements'])
