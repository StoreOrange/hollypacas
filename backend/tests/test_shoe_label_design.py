import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from app.core import shoe_label_design as design

class LabelDesignTests(unittest.TestCase):
    def test_default_size_and_pdf(self):
        data, count = design.render([{'name':'Zapato', 'code':'ABC123', 'quantity':2}], design.DEFAULT)
        self.assertTrue(data.startswith(b'%PDF'))
        self.assertEqual(count,2)
        self.assertIn(b'/MediaBox [ 0 0 141.7323 56.69291 ]', data)

    def test_custom_size_and_persistence(self):
        value=copy.deepcopy(design.DEFAULT)
        value.update(width=60,height=60,font='Courier')
        value['fields']['name']['size']=6
        with tempfile.TemporaryDirectory() as directory, patch.object(design,'PATH',Path(directory)/'design.json'):
            design.save(value)
            loaded=design.load()
            self.assertEqual(loaded['fields']['name']['size'],6)
            self.assertEqual(loaded['width'],60)
            data,_=design.render([{'code':'ABC123'}],loaded)
            self.assertNotIn(b'/MediaBox [ 0 0 141.7323 56.69291 ]',data)

    def test_compact_barcode_keeps_minimum_width_and_quiet_zone(self):
        value=copy.deepcopy(design.DEFAULT)
        value.update(barcode_width=.25,barcode_max_width=56,rotation=180)
        barcode=design.build_barcode('ZAP-001-38',value)
        self.assertLessEqual(barcode.width,56*design.mm)
        self.assertGreaterEqual(barcode.barWidth,0.21*design.mm)
        self.assertGreaterEqual(barcode.lquiet,10*barcode.barWidth)
        with self.assertRaises(ValueError):design.build_barcode('X'*100,value)
        with patch.object(design.canvas, 'Canvas', wraps=design.canvas.Canvas) as factory:
            from unittest.mock import MagicMock
            pdf=MagicMock();factory.return_value=pdf
            design.render([{'code':'ABC123'}],value)
            pdf.rotate.assert_called_once_with(180)

    def test_side_fields_share_row_without_collision(self):
        result=design.layout({'name':'Nombre','code':'ABC123','reference':'Ref. ABC','price':'C$ 100'},design.DEFAULT)
        self.assertEqual(result['errors'],[])
        nodes={n['id']:n for n in result['nodes']}
        self.assertEqual(nodes['reference']['y'],nodes['price']['y'])
        self.assertAlmostEqual(nodes['reference']['left'],2)
        self.assertAlmostEqual(nodes['price']['left']+nodes['price']['width'],48)

    def test_drag_coordinates_persist_and_drive_pdf(self):
        d=copy.deepcopy(design.DEFAULT)
        d['fields']['name'].update(x=26.5,top=3.4)
        d['barcode_x']=26
        result=design.layout({'name':'Nombre','code':'ABC123'},d)
        self.assertEqual(result['errors'],[])
        name=next(n for n in result['nodes'] if n['id']=='name')
        self.assertAlmostEqual(name['x'],26.5)
        self.assertAlmostEqual(name['y'],3.4)
        with tempfile.TemporaryDirectory() as directory, patch.object(design,'PATH',Path(directory)/'design.json'):
            design.save(d)
            self.assertEqual(design.load()['fields']['name']['x'],26.5)
        with patch.object(design.canvas,'Canvas') as factory:
            design.render([{'name':'Nombre','code':'ABC123'}],d)
            args=next(call.args for call in factory.return_value.drawString.call_args_list if call.args[2]=='Nombre')
            self.assertAlmostEqual(args[0],name['left']*design.mm)
            self.assertAlmostEqual(args[1],(d['height']-3.4)*design.mm)

    def test_overlap_in_two_dimensions_and_margin_rejected(self):
        for x,top in [(25,14.3),(1,3.4),(25,2)]:
            d=copy.deepcopy(design.DEFAULT)
            d['fields']['name'].update(x=x,top=top)
            result=design.layout({'name':'Nombre','code':'ABC123'},d)
            self.assertTrue(result['errors'])
            with self.assertRaises(ValueError):design.render([{'name':'Nombre','code':'ABC123'}],d)

    def test_long_text_is_not_silently_truncated(self):
        d=copy.deepcopy(design.DEFAULT)
        with self.assertRaises(ValueError):
            design.render([{'name':'W'*400,'code':'ABC'}],d)

    def test_reject_bad_dimensions_fonts_and_overflow(self):
        for key,value in [('width',float('nan')),('height',-1),('font','missing'),('barcode_height',99)]:
            config=copy.deepcopy(design.DEFAULT);config[key]=value
            with self.assertRaises(ValueError):design.render([{'code':'ABC123'}],config)

if __name__=='__main__': unittest.main()
