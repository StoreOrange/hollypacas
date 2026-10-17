"""El rol motorizado de Pacas Holl tiene un espacio exclusivo, aun con roles adicionales."""
from jose import jwt,JWTError
from sqlalchemy import func
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse,RedirectResponse
from ..database import get_session_local
from ..models.user import User,Role
from .security import SECRET_KEY,ALGORITHM

MESSAGE='Tu rol motorizado solo tiene acceso al módulo Motorizado.'


def is_holl():
    from ..routers import web
    return web._is_pacasholl_company()


def restricted(user,company=None):
    holl=is_holl() if company is None else company
    return bool(holl and any((role.name or '').strip().lower()=='motorizado' for role in user.roles))


def allowed_path(path):
    return path=='/motorizado' or path.startswith('/motorizado/') or path.startswith('/static/') or path in {'/login','/logout','/auth/login'}


def token_is_restricted(tokens):
    emails=set()
    for token in tokens:
        try:
            payload=jwt.decode(token,SECRET_KEY,algorithms=[ALGORITHM])
            if isinstance(payload.get('sub'),str):emails.add(payload['sub'].lower())
        except JWTError:continue
    if not emails:return False
    with get_session_local()() as db:
        return db.query(User.id).filter(func.lower(User.email).in_(emails),User.is_active.is_(True),User.roles.any(func.lower(Role.name)=='motorizado')).first() is not None


async def enforce_scope(request,call_next):
    if not is_holl() or allowed_path(request.url.path):return await call_next(request)
    tokens={request.cookies.get('access_token') or ''}
    authorization=request.headers.get('authorization','')
    if authorization.lower().startswith('bearer '):tokens.add(authorization[7:].strip())
    tokens.discard('')
    if tokens and await run_in_threadpool(token_is_restricted,tokens):
        if request.method in {'GET','HEAD'} and not request.url.path.startswith('/auth/') and 'application/json' not in request.headers.get('accept','').lower():
            return RedirectResponse('/motorizado',status_code=303,headers={'Cache-Control':'no-store'})
        return JSONResponse({'detail':MESSAGE},status_code=403,headers={'Cache-Control':'no-store'})
    return await call_next(request)
