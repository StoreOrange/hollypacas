from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..core.deps import get_current_user, get_db, require_admin
from ..core.security import create_access_token, hash_password, verify_password
from ..models.user import User
from ..schemas.user import UserCreate, UserResponse

router = APIRouter(prefix="/auth", tags=["Authentication"])


class LoginData(BaseModel):
    email: str
    password: str


@router.post("/register", response_model=UserResponse)
def register(
    user: UserCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    existing = db.query(User).filter(User.email == user.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email ya registrado")

    new_user = User(
        email=user.email,
        full_name=user.full_name,
        hashed_password=hash_password(user.password),
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


@router.post("/login")
def login(data: LoginData, db: Session = Depends(get_db)):
    identifier = data.email.strip().lower()
    user = db.query(User).filter(
        (func.lower(User.email) == identifier)
        | (func.lower(User.full_name) == identifier)
    ).first()
    if not user:
        raise HTTPException(status_code=400, detail="Usuario no encontrado")
    if not verify_password(data.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Contrasena incorrecta")

    role_names = {role.name for role in user.roles}
    if "administrador" not in role_names:
        raise HTTPException(status_code=403, detail="Acceso denegado")

    token = create_access_token({"sub": user.email})
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user.id, "email": user.email, "full_name": user.full_name},
    }


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)):
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Usuario inactivo"
        )
    return current_user
