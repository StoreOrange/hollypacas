import unittest
from unittest.mock import patch
from datetime import timedelta
from app.core import audit_access as a
from app.core.security import create_access_token

class AccessTests(unittest.TestCase):
 def test_token_is_bound_to_login_user_purpose_and_expiry(self):
  token=create_access_token({"sub":a.session_id(1,"login"),"purpose":"holl-audit"})
  self.assertTrue(a.unlocked(token,1,"login"))
  self.assertFalse(a.unlocked(token,2,"login"))
  self.assertFalse(a.unlocked(token,1,"new-login"))
  self.assertFalse(a.unlocked('tampered',1,"login"))
  expired=create_access_token({"sub":a.session_id(1,"login"),"purpose":"holl-audit"},expires_delta=timedelta(seconds=-10))
  self.assertFalse(a.unlocked(expired,1,"login"))
  ordinary=create_access_token({"sub":a.session_id(1,"login")})
  self.assertFalse(a.unlocked(ordinary,1,"login"))
 def test_wrong_password_is_limited(self):
  with patch.object(a,'_attempts',{}):
   for i in range(5):self.assertIn('incorrecta',a.unlock('wrong',99,'login')[1])
   self.assertIn('5 minutos',a.unlock('wrong',99,'login')[1])
