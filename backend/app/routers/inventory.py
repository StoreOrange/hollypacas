from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..core.deps import get_db, require_admin
from ..models.inventory import Linea, Producto, SaldoProducto, Segmento
from ..schemas.inventory import (
    LineaCreate,
    LineaResponse,
    ProductoCreate,
    ProductoResponse,
    ProductoUpdate,
    SegmentoCreate,
    SegmentoResponse,
)

router = APIRouter(prefix="/inventory", tags=["Inventory"])


@router.get("/catalogs", response_model=dict)
def get_catalogs(db: Session = Depends(get_db), _: None = Depends(require_admin)):
    lineas = db.query(Linea).order_by(Linea.linea).all()
    segmentos = db.query(Segmento).order_by(Segmento.segmento).all()
    return {
        "lineas": [LineaResponse.model_validate(linea) for linea in lineas],
        "segmentos": [SegmentoResponse.model_validate(seg) for seg in segmentos],
    }


@router.post("/lineas", response_model=LineaResponse)
def create_linea(
    payload: LineaCreate,
    db: Session = Depends(get_db),
    _: None = Depends(require_admin),
):
    exists = db.query(Linea).filter(Linea.cod_linea == payload.cod_linea).first()
    if exists:
        raise HTTPException(status_code=400, detail="Codigo de linea ya existe")
    linea = Linea(
        cod_linea=payload.cod_linea,
        linea=payload.linea,
        activo=payload.activo,
    )
    db.add(linea)
    db.commit()
    db.refresh(linea)
    return linea


@router.post("/segmentos", response_model=SegmentoResponse)
def create_segmento(
    payload: SegmentoCreate,
    db: Session = Depends(get_db),
    _: None = Depends(require_admin),
):
    exists = db.query(Segmento).filter(Segmento.segmento == payload.segmento).first()
    if exists:
        raise HTTPException(status_code=400, detail="Segmento ya existe")
    segmento = Segmento(segmento=payload.segmento)
    db.add(segmento)
    db.commit()
    db.refresh(segmento)
    return segmento


@router.patch("/lineas/{linea_id}/deactivate", response_model=LineaResponse)
def deactivate_linea(
    linea_id: int,
    db: Session = Depends(get_db),
    _: None = Depends(require_admin),
):
    linea = db.query(Linea).filter(Linea.id == linea_id).first()
    if not linea:
        raise HTTPException(status_code=404, detail="Linea no encontrada")
    linea.activo = False
    db.commit()
    db.refresh(linea)
    return linea


@router.get("/products", response_model=List[ProductoResponse])
def list_products(
    include_inactive: bool = Query(False),
    db: Session = Depends(get_db),
    _: None = Depends(require_admin),
):
    query = db.query(Producto)
    if not include_inactive:
        query = query.filter(Producto.activo.is_(True))
    return query.order_by(Producto.descripcion).all()


@router.post("/products", response_model=ProductoResponse)
def create_product(
    payload: ProductoCreate,
    db: Session = Depends(get_db),
    _: None = Depends(require_admin),
):
    exists = db.query(Producto).filter(Producto.cod_producto == payload.cod_producto).first()
    if exists:
        raise HTTPException(status_code=400, detail="Codigo de producto ya existe")

    producto = Producto(
        cod_producto=payload.cod_producto,
        descripcion=payload.descripcion,
        segmento_id=payload.segmento_id,
        linea_id=payload.linea_id,
        marca=payload.marca,
        precio_venta1=payload.precio_venta1,
        precio_venta2=payload.precio_venta2,
        precio_venta3=payload.precio_venta3,
        activo=payload.activo,
        servicio_producto=payload.servicio_producto,
        costo_producto=payload.costo_producto,
        referencia_producto=payload.referencia_producto,
        usuario_registro=payload.usuario_registro,
        maquina_registro=payload.maquina_registro,
    )
    db.add(producto)
    db.flush()

    saldo = SaldoProducto(producto_id=producto.id, existencia=payload.existencia)
    db.add(saldo)
    db.commit()
    db.refresh(producto)
    return producto


@router.put("/products/{product_id}", response_model=ProductoResponse)
def update_product(
    product_id: int,
    payload: ProductoUpdate,
    db: Session = Depends(get_db),
    _: None = Depends(require_admin),
):
    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if not producto:
        raise HTTPException(status_code=404, detail="Producto no encontrado")

    data = payload.dict(exclude_unset=True)
    existencia = data.pop("existencia", None)
    for key, value in data.items():
        setattr(producto, key, value)

    if existencia is not None:
        if producto.saldo:
            producto.saldo.existencia = existencia
        else:
            db.add(SaldoProducto(producto_id=producto.id, existencia=existencia))

    db.commit()
    db.refresh(producto)
    return producto


@router.patch("/products/{product_id}/deactivate", response_model=ProductoResponse)
def deactivate_product(
    product_id: int,
    db: Session = Depends(get_db),
    _: None = Depends(require_admin),
):
    producto = db.query(Producto).filter(Producto.id == product_id).first()
    if not producto:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    producto.activo = False
    db.commit()
    db.refresh(producto)
    return producto
