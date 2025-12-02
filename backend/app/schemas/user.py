from pydantic import BaseModel
from typing import List, Optional

class RoleBase(BaseModel):
    name: str

class RoleResponse(RoleBase):
    id: int
    class Config:
        orm_mode = True


class UserBase(BaseModel):
    email: str
    full_name: Optional[str] = None

class UserCreate(UserBase):
    password: str

class UserResponse(UserBase):
    id: int
    is_active: bool
    roles: List[RoleResponse] = []

    class Config:
        orm_mode = True
