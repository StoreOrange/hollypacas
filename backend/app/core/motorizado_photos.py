"""Fotos privadas y transaccionales: normalizadas, sin metadatos y con acceso por viaje."""
from hashlib import sha256
from io import BytesIO
from pathlib import PurePath
import warnings
from PIL import Image,ImageOps,UnidentifiedImageError
from ..models.motorizado import MotoTrip,MotoTripPhoto,MotoTripEvent
from .motorizado import allowed_trip

MAX_PHOTOS=6
MAX_UPLOAD=8*1024*1024


def prepare_uploads(uploads):
    files=[upload for upload in uploads if getattr(upload,'filename',None)]
    if len(files)>MAX_PHOTOS:raise ValueError('Puedes adjuntar hasta 6 fotos por viaje')
    prepared=[]
    for upload in files:
        raw=upload.file.read(MAX_UPLOAD+1)
        if len(raw)>MAX_UPLOAD:raise ValueError('Cada foto admite hasta 8 MB')
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('error',Image.DecompressionBombWarning)
                with Image.open(BytesIO(raw)) as check:
                    if check.format not in {'JPEG','PNG','WEBP'} or check.width*check.height>24_000_000:raise ValueError('Usa fotos JPG, PNG o WEBP de hasta 24 megapíxeles')
                    check.verify()
                with Image.open(BytesIO(raw)) as source:
                    image=ImageOps.exif_transpose(source)
                    image.thumbnail((1800,1800))
                    if image.mode in {'RGBA','LA'} or 'transparency' in image.info:
                        rgba=image.convert('RGBA');rgb=Image.new('RGB',rgba.size,'white');rgb.paste(rgba,mask=rgba.getchannel('A'))
                    else:rgb=image.convert('RGB')
                    out=BytesIO();rgb.save(out,format='JPEG',quality=85,optimize=True)
                    data=out.getvalue()
        except (UnidentifiedImageError,OSError,Image.DecompressionBombError,Image.DecompressionBombWarning):raise ValueError('No se pudo leer la foto. Selecciona una imagen JPG, PNG o WEBP válida')
        prepared.append((PurePath(upload.filename.replace('\\','/')).name[:180] or 'Foto',sha256(data).hexdigest(),data))
    return prepared


def attach(db,user,trip,prepared):
    from ..routers import web
    allowed_trip(db,user,trip)
    if not prepared:return 0
    trip=db.query(MotoTrip).filter_by(id=trip.id).populate_existing().with_for_update().one()
    known={r[0] for r in db.query(MotoTripPhoto.digest).filter_by(trip_id=trip.id)}
    additions=[]
    for item in prepared:
        if item[1] not in known:known.add(item[1]);additions.append(item)
    if len(known)>MAX_PHOTOS:raise ValueError('Este viaje admite un máximo de 6 fotos. Ya hay soportes adjuntos')
    for name,digest,data in additions:db.add(MotoTripPhoto(trip_id=trip.id,uploaded_by_id=user.id,original_name=name,digest=digest,data=data,created_at=web.local_now_naive()))
    if additions:db.add(MotoTripEvent(trip_id=trip.id,actor_id=user.id,action='FOTOS',comment=f'{len(additions)} foto(s) de soporte adjunta(s)',created_at=web.local_now_naive()))
    db.flush();return len(additions)
