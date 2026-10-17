import asyncio,unittest
from unittest.mock import patch,AsyncMock
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse
from sqlalchemy.orm import Session
from app.core import motorizado_access as guard,motorizado as service
from app.core.security import create_access_token
from app.models.user import Role,Permission
from app.routers import web
from tests import test_motorizado as fixture


class TestClient:
    """Transporte ASGI mínimo: sin servidor ni dependencias HTTP adicionales."""
    def __init__(self,app):self.app=app
    def request(self,method,path,cookies=None,headers=None,**kwargs):
        import json
        from types import SimpleNamespace
        async def run():
            request_headers={key.lower():value for key,value in (headers or {}).items()}
            if cookies:request_headers['cookie']='; '.join(f'{key}={value}' for key,value in cookies.items())
            scope={'type':'http','asgi':{'version':'3.0'},'http_version':'1.1','method':method,'scheme':'http','path':path,'raw_path':path.encode(),'query_string':b'','root_path':'','headers':[(key.encode(),value.encode()) for key,value in request_headers.items()],'server':('test',80),'client':('127.0.0.1',1234)}
            messages=[];started=False;finished=asyncio.Event()
            async def receive():
                nonlocal started
                if not started:started=True;return {'type':'http.request','body':b'','more_body':False}
                await finished.wait();return {'type':'http.disconnect'}
            async def send(message):
                messages.append(message)
                if message['type']=='http.response.body' and not message.get('more_body'):finished.set()
            await self.app(scope,receive,send)
            head=next(message for message in messages if message['type']=='http.response.start')
            body=b''.join(message.get('body',b'') for message in messages if message['type']=='http.response.body')
            return SimpleNamespace(status_code=head['status'],headers={key.decode():value.decode() for key,value in head['headers']},json=lambda:json.loads(body))
        return asyncio.run(run())
    def get(self,path,**kwargs):return self.request('GET',path,**kwargs)
    def post(self,path,**kwargs):return self.request('POST',path,**kwargs)


class MotoAccessTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixture.MotoTests();self.fixture.setUp();self.addCleanup(self.fixture.tearDown)

    def test_extra_roles_and_permissions_do_not_expand_rider_access(self):
        user=self.fixture.user;user.roles.append(Role(name='administrador'))
        user.roles[0].permissions.append(Permission(name='access.accounting'))
        self.assertTrue(guard.restricted(user,company=True))
        self.assertEqual(service.roles(user),{'motorizado'})
        self.assertFalse(web._has_permission(user,'access.accounting'))
        request=self.fixture.request() if hasattr(self.fixture,'request') else Request({'type':'http','method':'GET','path':'/motorizado','headers':[]})
        web._set_request_permissions(request,user)
        self.assertEqual(request.state.permission_names,{'menu.motorizado'})
        self.assertTrue(request.state.motorizado_only)

    def test_live_token_lookup_reads_current_role_cookie_and_bearer(self):
        user=self.fixture.user
        token=create_access_token({'sub':user.email})
        with patch.object(guard,'get_session_local',return_value=lambda:Session(self.fixture.engine)):
            self.assertTrue(guard.token_is_restricted({token}))
            self.assertFalse(guard.token_is_restricted({'invalid'}))
            self.assertFalse(guard.token_is_restricted({create_access_token({'sub':self.fixture.seller.email})}))
            user.roles=[];self.fixture.db.commit()
            self.assertFalse(guard.token_is_restricted({token}))

    def test_seller_keeps_own_identity_and_other_companies_unaffected(self):
        self.assertFalse(guard.restricted(self.fixture.user,company=False))
        self.assertFalse(guard.restricted(self.fixture.seller,company=True))
        self.assertEqual(service.roles(self.fixture.seller),{'vendedor'})


class MotoBoundaryTests(unittest.TestCase):
    def setUp(self):
        app=FastAPI()
        app.middleware('http')(guard.enforce_scope)
        @app.api_route('/{path:path}',methods=['GET','POST','DELETE','PUT','HEAD'])
        def every_path(path):return {'executed':True}
        self.client=TestClient(app)
        self.company=patch.object(guard,'is_holl',return_value=True);self.company.start();self.addCleanup(self.company.stop)
        self.lookup=patch.object(guard,'token_is_restricted',side_effect=lambda tokens:'rider-token' in tokens);self.lookup.start();self.addCleanup(self.lookup.stop)

    def test_navigation_returns_to_own_module_instead_of_opening_other_pages(self):
        for path in ['/','/home','/sales','/m/preventas','/accounting','/data/users','/motorizado-other']:
            result=self.client.get(path,cookies={'access_token':'rider-token'},follow_redirects=False)
            self.assertEqual(result.status_code,303,path);self.assertEqual(result.headers['location'],'/motorizado')

    def test_post_and_api_access_blocked_before_endpoint_executes(self):
        for path in ['/sales','/sales/automatico-pos/station','/auth/register','/inventory/productos']:
            result=self.client.post(path,cookies={'access_token':'rider-token'})
            self.assertEqual(result.status_code,403,path);self.assertNotIn('executed',result.json())
        result=self.client.get('/accounting',headers={'Authorization':'Bearer rider-token','Accept':'application/json'})
        self.assertEqual(result.status_code,403)
        result=self.client.get('/auth/me',headers={'Authorization':'Bearer rider-token'})
        self.assertEqual(result.status_code,403)

    def test_module_assets_and_logout_stay_available(self):
        for path in ['/motorizado','/motorizado/123/estado','/static/js/motorizado.js','/logout','/login','/auth/login']:
            result=self.client.get(path,cookies={'access_token':'rider-token'})
            self.assertEqual(result.status_code,200,path)
        self.assertEqual(self.client.post('/motorizado/123/estado',cookies={'access_token':'rider-token'}).status_code,200)

    def test_seller_normal_access_and_other_companies_not_restricted(self):
        self.assertEqual(self.client.get('/m/preventas',cookies={'access_token':'seller-token'}).status_code,200)
        with patch.object(guard,'is_holl',return_value=False):
            self.assertEqual(self.client.get('/sales',cookies={'access_token':'rider-token'}).status_code,200)

    def test_bearer_rider_is_not_bypassed_by_another_cookie(self):
        result=self.client.post('/auth/register',cookies={'access_token':'admin-token'},headers={'Authorization':'Bearer rider-token'})
        self.assertEqual(result.status_code,403)
