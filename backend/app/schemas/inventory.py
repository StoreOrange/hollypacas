from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class LineaBase(BaseModel):
    cod_linea: str
    linea: str
    activo: bool

    class Config:
        from_attributes = True


class LineaResponse(LineaBase):
    id: int

    class Config:
        from_attributes = True


class SegmentoBase(BaseModel):
    segmento: str

    class Config:
        from_attributes = True


class SaldoBase(BaseModel):
    existencia: float

    class Config:
        from_attributes = True


class SegmentoResponse(SegmentoBase):
    id: int

    class Config:
        from_attributes = True


class LineaCreate(BaseModel):
    cod_linea: str
    linea: str
    activo: bool = True


class SegmentoCreate(BaseModel):
    segmento: str


class ProductoBase(BaseModel):
    cod_producto: str
    descripcion: str
    segmento_id: Optional[int] = None
    linea_id: Optional[int] = None
    marca: Optional[str] = None
    precio_venta1: float = 0
    precio_venta2: float = 0
    precio_venta3: float = 0
    activo: bool = True
    servicio_producto: bool = False
    costo_producto: float = 0
    referencia_producto: Optional[str] = None
    usuario_registro: Optional[str] = None
    maquina_registro: Optional[str] = None


class ProductoCreate(ProductoBase):
    existencia: float = 0


class ProductoUpdate(BaseModel):
    descripcion: Optional[str] = None
    segmento_id: Optional[int] = None
    linea_id: Optional[int] = None
    marca: Optional[str] = None
    precio_venta1: Optional[float] = None
    precio_venta2: Optional[float] = None
    precio_venta3: Optional[float] = None
    activo: Optional[bool] = None
    servicio_producto: Optional[bool] = None
    costo_producto: Optional[float] = None
    referencia_producto: Optional[str] = None
    existencia: Optional[float] = None


class ProductoResponse(ProductoBase):
    id: int
    registro: Optional[datetime] = None
    ultima_modificacion: Optional[datetime] = None
    saldo: Optional[SaldoBase] = None
    linea: Optional[LineaResponse] = None
    segmento: Optional[SegmentoResponse] = None

    class Config:
        from_attributes = True
