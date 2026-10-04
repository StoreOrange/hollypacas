import unittest
from unittest.mock import patch
from types import SimpleNamespace
from fastapi import HTTPException
from app.core.local_label_printing import printers, submit_pdf, require_local

class PrintingTests(unittest.TestCase):
    @patch('app.core.local_label_printing._run')
    def test_macos_spanish_printers(self, run):
        run.return_value = ('la impresora Zebra_Technologies_ZTC_GK420t está inactiva. activada desde Tue Sep 29\n'
                            'la impresora Pausada desactivada desde Tue Sep 29\n')
        result = printers()
        self.assertEqual(result[0]['id'], 'Zebra_Technologies_ZTC_GK420t')
        self.assertTrue(result[0]['enabled'])
        self.assertFalse(result[1]['enabled'])

    @patch('app.core.local_label_printing._run')
    def test_printer_states(self, run):
        run.return_value='printer Zebra is idle. enabled since today\nprinter Offline disabled since today\n'
        self.assertEqual([p['enabled'] for p in printers()], [True, False])

    @patch('app.core.local_label_printing._run')
    def test_exact_size_and_single_job(self, run):
        run.side_effect=['printer Zebra is idle. enabled since today\n','request id is Zebra-42 (1 file(s))']
        self.assertEqual(submit_pdf(b'%PDF-test', 'Zebra'), 'Zebra-42')
        args=run.call_args.args[0]
        self.assertIn('PageSize=Custom.50x20mm',args)
        self.assertIn('print-scaling=none',args)
        self.assertEqual(args[args.index('-n')+1], '1')

    @patch('app.core.local_label_printing._run')
    def test_unknown_destination_never_submits(self, run):
        run.return_value='printer Zebra is idle. enabled since today\n'
        with self.assertRaises(HTTPException): submit_pdf(b'pdf','other')
        self.assertEqual(run.call_count,1)

    def test_local_origin_required(self):
        for host, origin in [('192.168.1.3','http://localhost:8001'),('127.0.0.1','https://external.example')]:
            request=SimpleNamespace(client=SimpleNamespace(host=host),headers={'origin':origin,'host':'localhost:8001','x-requested-with':'fetch'})
            with self.assertRaises(HTTPException): require_local(request)
        require_local(SimpleNamespace(client=SimpleNamespace(host='127.0.0.1'),headers={'origin':'http://localhost:8001','host':'localhost:8001','x-requested-with':'fetch'}))

if __name__ == '__main__': unittest.main()
