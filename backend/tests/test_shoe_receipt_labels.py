import unittest
from decimal import Decimal
from types import SimpleNamespace as N
from app.core.shoe_receipt_labels import receipt_rows,selected_rows,print_rows,units


def fixture():
    items=[];ident=1
    for model,sizes in [(1,[5,5,6]),(2,[2])]:
        product=N(id=model,cod_producto=f'MODELO-{model}',descripcion=f'Zapato {model}',precio_venta1=750)
        for color,count in enumerate(sizes,1):
            for talla in range(1,count+1):
                variant=N(id=ident,cod_variante=f'M{model}-C{color}-T{talla}',color=N(nombre=f'Color {color}'),talla=str(talla))
                items.append(N(producto=product,variante=variant,cantidad=Decimal(talla+color)))
                ident+=1
    return N(id=1,items=items)


class ReceiptLabelTests(unittest.TestCase):
    def test_unequal_sizes_and_quantities_grouped_exactly(self):
        receipt=fixture();rows=receipt_rows(receipt,lambda i:f'V{i:06d}')
        self.assertEqual(len(rows),18)
        self.assertEqual(sum(r['quantity'] for r in rows),sum(int(i.cantidad) for i in receipt.items))
        for item in receipt.items:
            row=next(r for r in rows if r['variant_id']==item.variante.id)
            self.assertEqual(row['quantity'],int(item.cantidad))
            self.assertEqual(row['product_id'],item.producto.id)
        self.assertEqual(sum(r['product_id']==1 for r in rows),16)

    def test_repeated_variant_lines_sum_only_that_variant(self):
        receipt=fixture();receipt.items.append(N(**{**receipt.items[0].__dict__,'cantidad':Decimal(7)}))
        rows=receipt_rows(receipt,lambda i:f'V{i:06d}')
        self.assertEqual(next(r['quantity'] for r in rows if r['variant_id']==1),9)
        self.assertEqual(len(rows),18)

    def test_selection_keeps_receipt_quantities_and_short_codes(self):
        rows=receipt_rows(fixture(),lambda i:f'V{i:06d}')
        selected=selected_rows(rows,[1,16,18])
        self.assertEqual({r['variant_id'] for r in selected},{1,16,18})
        self.assertEqual([r['quantity'] for r in print_rows(selected)],[r['quantity'] for r in selected])
        self.assertEqual(print_rows(selected)[0]['code'],'V000001')
        for ids in [[],[999],[1,1],[True],['1']]:
            with self.assertRaises(ValueError):selected_rows(rows,ids)

    def test_never_round_or_truncate_units(self):
        for value in ['1.5','NaN','Infinity',-1,'bad']:
            with self.assertRaises(ValueError):units(value)
        self.assertEqual(units('6.00'),6)
        rows=receipt_rows(fixture(),lambda i:str(i));rows[0]['quantity']=5001
        with self.assertRaises(ValueError):selected_rows(rows,[rows[0]['variant_id']])

    def test_reopening_does_not_change_receipt(self):
        receipt=fixture();before=[i.cantidad for i in receipt.items]
        a=receipt_rows(receipt,lambda i:str(i));selected_rows(a,[1])
        b=receipt_rows(receipt,lambda i:str(i))
        self.assertEqual(a,b);self.assertEqual(before,[i.cantidad for i in receipt.items])

if __name__=='__main__':unittest.main()

class ReceiptRoutesTests(unittest.IsolatedAsyncioTestCase):
    async def test_payload_cannot_override_quantities_or_add_foreign_variants(self):
        from unittest.mock import patch,MagicMock
        from fastapi import HTTPException
        from app.routers import web
        class Request:
            headers={'x-requested-with':'fetch'}
            async def json(self):return self.payload
        request=Request();db=MagicMock();receipt=fixture()
        with patch.object(web,'_shoe_receipt_for_labels',return_value=receipt):
            for payload in [{'variant_ids':[1], 'quantity':999},{'variant_ids':[999]},{'variant_ids':[1,1]}]:
                request.payload=payload
                with self.assertRaises(HTTPException) as caught:
                    await web.inventory_ingreso_label_job(request,1,db,None)
                self.assertEqual(caught.exception.status_code,400)
            request.payload={'variant_ids':[1,16,18],'mode':'pdf'}
            response=await web.inventory_ingreso_label_job(request,1,db,None)
            expected=sum(int(i.cantidad) for i in receipt.items if i.variante.id in {1,16,18})
            self.assertEqual(int(response.headers['X-Total-Labels']),expected)
            self.assertTrue(response.body.startswith(b'%PDF'))
            self.assertEqual(float(response.headers['X-Label-Width-Mm']),50.0)
            self.assertIn(f'/Count {expected}'.encode(),response.body)
        db.commit.assert_not_called();db.add.assert_not_called()

    async def test_fractional_intake_rejected_before_stock_writes(self):
        from unittest.mock import patch,MagicMock
        from app.routers import web
        class Request:
            async def form(self):return {'tipo_id':'1','bodega_id':'1','fecha':'2026-09-29','codigo_base':'TEST',
                'linea_id':'1','marca_nombre':'TEST','costo_unitario':'10','precio_unitario':'20','matrix_json':'{"1":{"38":1.5}}'}
        db=MagicMock();db.query.return_value.filter.return_value.first.return_value=N(requiere_proveedor=False)
        with patch.object(web,'_enforce_permission'),patch.object(web,'_is_shoes_mode',return_value=True):
            response=await web.inventory_create_ingreso_zapatos(Request(),db,N(full_name='Test'))
        self.assertEqual(response.status_code,303)
        self.assertIn('error=',response.headers['location'])
        db.add.assert_not_called();db.flush.assert_not_called();db.commit.assert_not_called()
