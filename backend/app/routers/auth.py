from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel

from ..database import SessionLocal
from ..models.user import User, Role
from ..schemas.user import UserCreate, UserResponse
from ..core.security import hash_password, verify_password, create_access_token

router = APIRouter(prefix="/auth", tags=["Authentication"])


# Obtener sesión de DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ===========================
#   MODELO PARA LOGIN JSON
# ===========================
class LoginData(BaseModel):
    email: str
    password: str


# ===========================
#       REGISTRO
# ===========================
@router.post("/register", response_model=UserResponse)
def register(user: UserCreate, db: Session = Depends(get_db)):

    existing = db.query(User).filter(User.email == user.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email ya está registrado")

    hashed = hash_password(user.password)

    new_user = User(
        email=user.email,
        full_name=user.full_name,
        hashed_password=hashed,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return new_user


# ===========================
#           LOGIN
# ===========================
@router.post("/login")
def login(data: LoginData, db: Session = Depends(get_db)):

    email = data.email
    password = data.password

    user = db.query(User).filter(User.email == email).first()

    if not user:
        raise HTTPException(status_code=400, detail="Usuario no encontrado")

    if not verify_password(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Contraseña incorrecta")

    token = create_access_token({"sub": email})

    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "full_name": user.full_name
        }
    }
