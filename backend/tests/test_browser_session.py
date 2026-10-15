import unittest
from datetime import timedelta,datetime,timezone
from types import SimpleNamespace
from unittest.mock import patch,MagicMock
from starlette.requests import Request
from starlette.responses import Response
from jose import jwt
from app.core.browser_session import persist_browser_session,clear_browser_session


def request(host,scheme='https'):
    return Request({'type':'http','scheme':scheme,'server':(host,443),'path':'/login','headers':[(b'host',host.encode())],'query_string':b''})


class BrowserSessionTests(unittest.TestCase):
    def test_persistent_cookie_has_no_domain_and_ninety_days(self):
        for host in ('hollywoodpacas.ovh','www.hollywoodpacas.ovh','127.0.0.1','localhost'):
            with self.subTest(host=host):
                response=Response();persist_browser_session(response,request(host),'test',timedelta(days=90))
                cookie=response.headers.getlist('set-cookie')[-1]
                self.assertNotIn('domain=',cookie.lower());self.assertIn('Max-Age=7776000',cookie)
                self.assertIn('HttpOnly',cookie);self.assertIn('Secure',cookie);self.assertIn('SameSite=lax',cookie)
                self.assertIn('expires=',cookie.lower())

    def test_local_http_cookie_is_usable(self):
        response=Response();persist_browser_session(response,request('localhost','http'),'test',timedelta(days=90))
        self.assertNotIn('Secure',response.headers.getlist('set-cookie')[-1])

    def test_removes_legacy_cookie_on_bare_and_www_hosts(self):
        for host in ('hollywoodpacas.ovh','www.hollywoodpacas.ovh'):
            response=Response();clear_browser_session(response,request(host))
            cookies=response.headers.getlist('set-cookie')
            self.assertEqual(len(cookies),2)
            self.assertTrue(all('Max-Age=0' in value for value in cookies))
            self.assertIn('Domain=.hollywoodpacas.ovh',cookies[-1]);self.assertNotIn('Domain=.ovh;',cookies[-1])

    def test_login_token_and_cookie_expire_together(self):
        from app.routers import web
        from app.core.security import SECRET_KEY,ALGORITHM
        db=MagicMock();db.query.return_value.filter.return_value.first.return_value=SimpleNamespace(email='test@example.invalid',hashed_password='test',is_active=True)
        with patch.object(web,'_is_pacasholl_company',return_value=True),patch.object(web,'_is_shoes_mode',return_value=False),patch.object(web,'verify_password',return_value=True):
            response=web.login_action(request('hollywoodpacas.ovh'),username='test',password='test',remember='1',db=db)
        cookie=response.headers.getlist('set-cookie')[-1]
        token=cookie.split('access_token=',1)[1].split(';',1)[0]
        payload=jwt.decode(token,SECRET_KEY,algorithms=[ALGORITHM])
        self.assertAlmostEqual(payload['exp']-datetime.now(timezone.utc).timestamp(),90*86400,delta=3)
        self.assertIn('Max-Age=7776000',cookie)
