import base64,hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError

spec=importlib.util.spec_from_file_location('pos_windows_agent',Path(__file__).resolve().parents[1]/'scripts'/'hollpacas_pos_agent.py')
agent_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(agent_module)


class WindowsAgentTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.path=Path(self.directory.name)/'hollpacas-pos.json'
        self.path.write_text(json.dumps({'api_url':'https://www.hollywoodpacas.ovh','station_id':'a'*32,'token':'test'}))
        self.agent=agent_module.Agent(self.path)
        pdf=b'%PDF-1.4\nTEST';self.job={'id':1,'claim_id':'b'*32,'copies':2,'pdf':base64.b64encode(pdf).decode(),'sha256':hashlib.sha256(pdf).hexdigest()}
    def tearDown(self):self.directory.cleanup()

    def test_pdf_copies_one_silent_command_and_ack(self):
        with patch.object(agent_module.subprocess,'run',return_value=SimpleNamespace(returncode=0)) as command,patch.object(self.agent,'api',return_value={'ok':True}) as api:
            self.agent.print_job(self.job,'EPSON TM-T20','SumatraPDF.exe')
            args=command.call_args.args[0];self.assertEqual(args[:5],['SumatraPDF.exe','-print-to','EPSON TM-T20','-print-settings','noscale,2x']);self.assertIn('-silent',args);self.assertEqual(command.call_count,1)
            self.assertEqual(api.call_args.args[1]['status'],'SENT');self.assertEqual(self.agent.state,{})

    def test_invalid_pdf_never_calls_printer(self):
        self.job['sha256']='bad'
        with patch.object(agent_module.subprocess,'run') as command,patch.object(self.agent,'api',return_value={'ok':True}) as api:
            self.agent.print_job(self.job,'EPSON','SumatraPDF.exe');command.assert_not_called();self.assertEqual(api.call_args.args[1]['status'],'FAILED')

    def test_failed_ack_persists_without_printing_again(self):
        with patch.object(agent_module.subprocess,'run',return_value=SimpleNamespace(returncode=0)) as command,patch.object(self.agent,'api',side_effect=OSError('Offline')):
            with self.assertRaises(OSError):self.agent.print_job(self.job,'EPSON','SumatraPDF.exe')
            self.assertEqual(command.call_count,1)
        restarted=agent_module.Agent(self.path)
        with patch.object(agent_module.subprocess,'run') as command,patch.object(restarted,'api',return_value={'ok':True}) as api:
            restarted.flush_ack();command.assert_not_called();self.assertEqual(api.call_args.args[1]['status'],'SENT')

    def test_interrupted_send_becomes_unknown(self):
        self.agent.state={'b'*32:{'id':1,'status':'INFLIGHT','message':'Iniciado'}}
        with patch.object(self.agent,'api',return_value={'ok':True}) as api:self.agent.flush_ack();self.assertEqual(api.call_args.args[1]['status'],'UNKNOWN')

    def test_stale_ack_does_not_block_a_new_manual_attempt(self):
        self.agent.state={'b'*32:{'id':1,'status':'SENT','message':'Enviado'}}
        with patch.object(self.agent,'api',side_effect=HTTPError('https://test',409,'Old claim',None,None)):
            self.agent.flush_ack();self.assertEqual(self.agent.state,{})

    def test_no_unencrypted_remote_service(self):
        self.path.write_text(json.dumps({'api_url':'http://example.com','station_id':'a'*32,'token':'test'}))
        with self.assertRaisesRegex(RuntimeError,'HTTPS'):agent_module.Agent(self.path)
