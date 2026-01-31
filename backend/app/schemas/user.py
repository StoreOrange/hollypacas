from pydantic import BaseModel
from typing import List, Optional

class RoleBase(BaseModel):
    name: str


class PermissionBase(BaseModel):
    name: str


class RoleResponse(RoleBase):
    id: int
    class Config:
        from_attributes = True


class PermissionResponse(PermissionBase):
    id: int
    class Config:
        from_attributes = True


class BranchBase(BaseModel):
    code: str
    name: str


class BranchResponse(BranchBase):
    id: int
    class Config:
        from_attributes = True


class UserBase(BaseModel):
    email: str
    full_name: Optional[str] = None

class UserCreate(UserBase):
    password: str

class UserResponse(UserBase):
    id: int
    is_active: bool
    roles: List[RoleResponse] = []
    branches: List[BranchResponse] = []
    permissions: List[PermissionResponse] = []

    class Config:
        from_attributes = True
