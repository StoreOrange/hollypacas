"""Additional password gate for the administrator sales-access audit."""
import hashlib
import hmac
import time
from threading import Lock
from .security import create_access_token, SECRET_KEY, ALGORITHM
from datetime import timedelta
from jose import jwt, JWTError

COOKIE = "holl_audit_access"
_SALT = "ddb97e0ff5df0be8fe704985590cee6c"
_HASH = "8b4612ac4c4c572bca08e65c421d17998b9ddf5bd54b2628475798d231789415"
_attempts = {}
_lock = Lock()

def session_id(user_id, login_token):
    return hashlib.sha256(f"{user_id}:{login_token}".encode()).hexdigest()

def unlocked(token, user_id, login_token):
    try:
        claims = jwt.decode(token or "", SECRET_KEY, algorithms=[ALGORITHM])
        return claims.get("purpose") == "holl-audit" and claims.get("sub") == session_id(user_id, login_token)
    except (JWTError, ValueError, TypeError):
        return False

def unlock(password, user_id, login_token):
    key = str(user_id)
    with _lock:
        now = time.monotonic()
        failures = [at for at in _attempts.get(key, []) if now-at < 300]
        if len(failures) >= 5:
            return None, "Demasiados intentos. Espera 5 minutos."
        valid = hmac.compare_digest(hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(_SALT), 600000).hex(), _HASH) if len(password) <= 128 else False
        if not valid:
            _attempts[key] = failures + [now]
            return None, "Contraseña incorrecta."
        _attempts.pop(key, None)
    return create_access_token({"sub": session_id(user_id, login_token), "purpose": "holl-audit"}, expires_delta=timedelta(minutes=15)), ""
