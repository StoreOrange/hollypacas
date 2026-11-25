"""Audit files and report permissions; temporary files and a mocked DB only."""
import json,os,tempfile,unittest
from datetime import date,datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch,MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.templating import Jinja2Templates
from app.core import hot_sales_audit as a
from app.routers import web as w

class AuditTests(unittest.TestCase):
 def setUp(self):self.temp=tempfile.TemporaryDirectory();self.folder=Path(self.temp.name)
 def tearDown(self):self.temp.cleanup();a._read_file.cache_clear()
 def write(self,prefix,rows,day='2026-11-23'):
  with (self.folder/(prefix+day+'.jsonl')).open('w') as f:
   for row in rows:f.write(json.dumps(row)+'\n')
 def mobile(self,t='09:00:00',user='a@test',**updates):
  return dict(recorded_at='2026-11-23T'+t+'-06:00',source='server',event='http_request',url={'path':a.TARGET},status=200,user=user,ip='127.0.0.1',device_declared='iPhone',**updates)
 def report(self,**kwargs):return a.build_report(date(2026,11,23),date(2026,11,23),folder=self.folder,**kwargs)
 def test_exact_target_success_and_back_separated(self):
  rows=[self.mobile(),self.mobile('09:10:00')]
  rows +=[{**self.mobile('09:11:00'),'status':403},{**self.mobile('09:12:00'),'url':{'path':'/sales'}},dict(recorded_at='2026-11-23T09:15:00-06:00',source='browser_unverified',event='page_view',url={'path':a.TARGET},navigation='back_forward',user='a@test')]
  self.write('mobile-',rows);r=self.report();self.assertEqual(r['opens'],2);self.assertEqual(r['rows'][1]['interval'],600)
  self.assertEqual(self.report(kind='todas')['returns'],1);self.assertEqual(self.report(kind='todas')['opens'],2)
 def test_two_sources_do_not_double_count_two_real_reloads(self):
  key=a.correlation('127.0.0.1','iPhone')
  self.write('mobile-',[self.mobile(),self.mobile('09:00:01')])
  self.write('hot-sales-',[{'recorded_at':f'2026-11-23T09:00:0{i}-06:00','source':'hot_sales_server','path':a.TARGET,'status':200,'user':'a@test','device':'Móvil','correlation':key,'access_id':str(i)} for i in [0,1]])
  r=self.report();self.assertEqual(r['opens'],2);self.assertEqual(r['rows'][1]['interval'],1)
 def test_accounts_not_mixed_and_unknown_has_no_invented_interval(self):
  self.write('mobile-',[self.mobile(),self.mobile('09:05:00','b@test'),self.mobile('09:10:00')])
  self.write('hot-sales-history-',[{'recorded_at':f'2026-11-23T10:0{i}:00-06:00','source':'nginx_history','path':a.TARGET,'status':200,'device':'PC'} for i in [0,1]])
  r=self.report();self.assertEqual(r['unknown'],2);self.assertEqual(r['rows'][2]['interval'],600)
  self.assertTrue(all(row['interval'] is None for row in r['rows'] if not row['user']))
  self.assertEqual(self.report(account='a@test')['opens'],2);self.assertEqual(self.report(device='PC')['opens'],2)
 def test_timezone_midnight_reset_malformed_and_updated_files(self):
  self.write('mobile-',[self.mobile('23:59:00')])
  self.write('mobile-',[{**self.mobile(),'recorded_at':'2026-11-24T06:01:00+00:00'}],'2026-11-24')
  with (self.folder/'mobile-2026-11-23.jsonl').open('a') as f:f.write('broken\n'+json.dumps({'recorded_at':None})+'\n')
  r=a.build_report(date(2026,11,23),date(2026,11,24),folder=self.folder)
  self.assertEqual(len(r['rows']),2);self.assertIsNone(r['rows'][1]['interval']);self.assertEqual(r['rows'][1]['at'].hour,0)
  self.write('mobile-',[self.mobile(),self.mobile('09:10:00')]);self.assertEqual(self.report()['opens'],2)
 def test_record_does_not_store_ip_tokens_or_browser_header(self):
  req=Request({'type':'http','method':'GET','path':a.TARGET,'headers':[(b'user-agent',b'Firefox/160'),(b'cookie',b'access_token=private')],'client':('10.1.2.3',42)})
  with patch.dict(os.environ,{'HOT_SALES_AUDIT_DIR':str(self.folder)}):a.record_access(req,SimpleNamespace(email='a@test',full_name='A'))
  payload=next(self.folder.glob('hot-sales-*')).read_text();self.assertNotIn('10.1.2.3',payload);self.assertNotIn('private',payload)
  self.assertEqual(json.loads(payload)['device'],'PC')
 def test_write_failure_does_not_break_report(self):
  with patch.object(a,'audit_directory',side_effect=OSError('read only')),patch.object(a.LOGGER,'exception'):
   a.record_access(SimpleNamespace(headers={},client=None),SimpleNamespace(email='a',full_name='A'))

class SnapshotTests(AuditTests):
 def context(self):
  return {'tasa':36.5,'default_currency':'USD','today_label':'23/11/2026',
          'total_day_all':100.125,'total_all':200.25,
          'branch_totals':[{'id':1,'name':'Central','total_day':80.0,'total_month':150.0},
                           {'id':2,'name':'Esteli','total_day':20.125,'total_month':50.25}]}
 def test_capture_preserves_each_displayed_card_and_six_decimal_conversion(self):
  snapshot=a.report_snapshot(self.context());self.assertEqual(snapshot['total_day'],{'usd':'100.13','cs':'3654.56'})
  self.assertEqual(snapshot['total_month'],{'usd':'200.25','cs':'7309.13'})
  self.assertEqual([b['name'] for b in snapshot['branches']],['Central','Esteli'])
  c=self.context();c['total_day_all']=1.1234567
  self.assertEqual(a.report_snapshot(c)['total_day'],{'usd':'1.12','cs':'41.01'})
 def test_stored_snapshot_is_immutable_and_legacy_is_not_reconstructed(self):
  context=self.context();req=Request({'type':'http','method':'GET','path':a.TARGET,'headers':[],'client':('127.0.0.1',1)})
  with patch.dict(os.environ,{'HOT_SALES_AUDIT_DIR':str(self.folder)}):a.record_access(req,SimpleNamespace(email='a@test',full_name='A'),context)
  context['total_day_all']=999999;context['branch_totals'][0]['total_day']=999999
  raw=json.loads(next(self.folder.glob('hot-sales-*')).read_text());self.assertEqual(raw['snapshot']['total_day']['usd'],'100.13')
  self.assertEqual(raw['snapshot']['branches'][0]['day']['usd'],'80.00')
  self.write('hot-sales-',[{**raw,'recorded_at':'2026-11-23T09:00:00-06:00'}]);self.write('mobile-',[self.mobile('08:00:00')])
  report=self.report();self.assertIsNone(report['rows'][0]['snapshot']);self.assertEqual(report['rows'][1]['snapshot']['total_day']['usd'],'100.13')
 def test_missing_and_invalid_capture_differ_from_true_zero(self):
  self.assertIsNone(a.read_snapshot(None));self.assertIsNone(a.read_snapshot({'version':1,'total_day':{'usd':'NaN','cs':'0'}}))
  context=self.context();context['total_day_all']=0
  self.assertEqual(a.read_snapshot(a.report_snapshot(context))['total_day']['usd'],'0.00')
 def test_no_scope_expansion_or_database_lookup(self):
  context=self.context();context['branch_totals']=context['branch_totals'][:1]
  snapshot=a.report_snapshot(context);self.assertEqual(len(snapshot['branches']),1);self.assertEqual(snapshot['branches'][0]['name'],'Central')

class AuditRouteTests(AuditTests):
 def setUp(self):
  super().setUp();self.user=SimpleNamespace(id=1,email='a@test',full_name='Admin',roles=[SimpleNamespace(name='administrador')],vendedor_id=None)
  self.db=MagicMock();self.db.query.return_value.all.return_value=[('a@test','Admin')]
  app=FastAPI();app.include_router(w.router);app.state.templates=Jinja2Templates(directory='app/templates')
  app.dependency_overrides[w.get_db]=lambda:self.db;app.dependency_overrides[w._require_admin_web]=lambda:self.user
  self.patches=[patch.object(w,'_is_hollpacas_mode',return_value=True),patch.object(w,'_enforce_permission'),patch.object(a,'audit_directory',return_value=self.folder)]
  for p in self.patches:p.start()
  self.client=TestClient(app)
 def tearDown(self):
  for p in reversed(self.patches):p.stop()
  self.client.close();super().tearDown()
 def test_admin_report_filters_and_private_cache(self):
  self.write('mobile-',[self.mobile(),self.mobile('09:10:00')]);r=self.client.get('/data/audit?start_date=2026-11-23&end_date=2026-11-23')
  self.assertEqual(r.status_code,200);self.assertEqual(r.context['report']['opens'],2);self.assertIn('10 min 0 s',r.text);self.assertIn('no-store',r.headers['cache-control'])
  self.db.commit.assert_not_called()
 def test_capture_visible_in_grid_and_old_access_stays_missing(self):
  snapshot=a.report_snapshot({'tasa':36.5,'default_currency':'USD','today_label':'23/11/2026','total_day_all':100,'total_all':200,'branch_totals':[{'id':1,'name':'Central','total_day':100,'total_month':200}]})
  self.write('hot-sales-',[{'recorded_at':'2026-11-23T09:00:00-06:00','source':'hot_sales_server','status':200,'path':a.TARGET,'user':'a@test','device':'PC','snapshot':snapshot}])
  self.write('mobile-',[self.mobile('08:00:00')]);response=self.client.get('/data/audit?start_date=2026-11-23&end_date=2026-11-23')
  self.assertEqual(response.status_code,200);self.assertIn('No capturado',response.text);self.assertIn('3,650.00 equivalente',response.text);self.assertIn('data-snapshot=',response.text)
 def test_explicit_admin_company_and_input_limits(self):
  self.user.roles=[SimpleNamespace(name='vendedor')];self.assertEqual(self.client.get('/data/audit').status_code,403)
  self.user.roles=[SimpleNamespace(name='administrador')]
  with patch.object(w,'_is_hollpacas_mode',return_value=False):self.assertEqual(self.client.get('/data/audit').status_code,403)
  for query in ['start_date=oops','start_date=2026-01-01&end_date=2026-12-31','device=other','kind=bad','page=oops']:
   self.assertEqual(self.client.get('/data/audit?'+query).status_code,400,query)

class HistoryImportTests(unittest.TestCase):
 def test_only_confirmed_desktop_history_and_no_identity_or_ip_leak(self):
  from scripts.import_hot_sales_history import import_history
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);logs=root/'logs';logs.mkdir();output=root/'output'
   base='1.2.3.4 - - [23/Nov/2026:10:00:00 -0600] "GET /sales/ventas-caliente HTTP/1.1" 200 123 "https://www.hollywoodpacas.ovh/sales" "Firefox/160"\n'
   (logs/'access.log').write_text(base+base+base.replace(' 200 ',' 302 ')+base.replace('hollywoodpacas.ovh','unrelated.example')+base.replace('Firefox/160','iPhone Safari'))
   result=import_history(logs,output,date(2026,11,23),date(2026,11,23))
   self.assertEqual(result,{'2026-11-23':2})
   payload=(output/'hot-sales-history-2026-11-23.jsonl').read_text();self.assertNotIn('1.2.3.4',payload);self.assertIsNone(json.loads(payload.splitlines()[0])['user'])
