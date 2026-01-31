from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from ..database import Base

user_roles = Table(
    "user_roles",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE")),
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE")),
)

role_permissions = Table(
    "role_permissions",
    Base.metadata,
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE")),
    Column("permission_id", Integer, ForeignKey("permissions.id", ondelete="CASCADE")),
)

user_branches = Table(
    "user_branches",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE")),
    Column("branch_id", Integer, ForeignKey("branches.id", ondelete="CASCADE")),
)


class Role(Base):
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, nullable=False)

    users = relationship("User", secondary=user_roles, back_populates="roles")
    permissions = relationship(
        "Permission", secondary=role_permissions, back_populates="roles"
    )


class Permission(Base):
    __tablename__ = "permissions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(80), unique=True, nullable=False)

    roles = relationship(
        "Role", secondary=role_permissions, back_populates="permissions"
    )


class Branch(Base):
    __tablename__ = "branches"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(40), unique=True, nullable=False)
    name = Column(String(80), unique=True, nullable=False)
    company_name = Column(String(120), nullable=True)
    ruc = Column(String(40), nullable=True)
    telefono = Column(String(40), nullable=True)
    direccion = Column(String(240), nullable=True)

    users = relationship("User", secondary=user_branches, back_populates="branches")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String(100))
    email = Column(String(120), unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    default_branch_id = Column(Integer, ForeignKey("branches.id"), nullable=True)
    default_bodega_id = Column(Integer, ForeignKey("bodegas.id"), nullable=True)

    roles = relationship("Role", secondary=user_roles, back_populates="users")
    branches = relationship("Branch", secondary=user_branches, back_populates="users")
    default_branch = relationship("Branch", foreign_keys=[default_branch_id])
    default_bodega = relationship("Bodega", foreign_keys=[default_bodega_id])

    @property
    def permissions(self):
        permission_map = {}
        for role in self.roles:
            for perm in role.permissions:
                permission_map[perm.id] = perm
        return list(permission_map.values())
