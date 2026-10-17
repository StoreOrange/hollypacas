import unittest
from io import BytesIO
from PIL import Image
from fastapi import HTTPException
from sqlalchemy import inspect,event
from starlette.datastructures import UploadFile,FormData
from app.core import motorizado as service,motorizado_photos as photos
from app.models.motorizado import MotoTripPhoto,MotoTripEvent,MotoTrip
from app.models.sales import AccountingEntry,ReciboCaja,CajaDiaria
from app.routers import motorizado as router
from tests import test_motorizado as fixture
from tests import test_pos_automation as common


class MotoPhotoTests(unittest.TestCase):
    def setUp(self):
        self.case=fixture.MotoTests();self.case.setUp();self.addCleanup(self.case.tearDown)
    def image(self,color='red',name='recibo.png'):
        out=BytesIO();Image.new('RGB',(80,60),color).save(out,format='PNG')
        return UploadFile(file=BytesIO(out.getvalue()),filename=name)
    def attach(self,trip,uploads,user=None):
        prepared=photos.prepare_uploads(uploads);count=photos.attach(self.case.db,user or self.case.user,trip,prepared);self.case.db.commit();return count

    def test_normalized_private_photos_before_and_after_delivery_and_void(self):
        trip=self.case.create();self.assertEqual(self.attach(trip,[self.image()]),1)
        service.transition(self.case.db,self.case.user,trip.id,'ENTREGADO');self.case.db.commit()
        self.assertEqual(self.attach(trip,[self.image('blue')]),1)
        self.assertEqual(self.attach(trip,[self.image('green')],self.case.seller),1)
        row=self.case.db.query(MotoTripPhoto).first();self.case.db.expire_all();row=self.case.db.get(MotoTripPhoto,row.id)
        self.assertIn('data',inspect(row).unloaded)
        request=common.AutoPosTests.request(self.case)
        with self.assertRaises(HTTPException):router.photo(row.id,request,self.case.db,self.case.other)
        response=router.photo(row.id,request,self.case.db,self.case.seller)
        self.assertEqual(response.media_type,'image/jpeg');self.assertIn('no-store',response.headers['cache-control'])
        with Image.open(BytesIO(response.body)) as image:self.assertEqual(image.format,'JPEG');self.assertFalse(image.getexif())
        self.assertEqual(self.case.db.query(ReciboCaja).count(),0);self.assertEqual(self.case.db.query(CajaDiaria).count(),0);self.assertEqual(self.case.db.query(AccountingEntry).count(),1)
        pending=self.case.create();service.transition(self.case.db,self.case.seller,pending.id,'ANULADO','Cancelado',True);self.case.db.commit()
        self.assertEqual(self.attach(pending,[self.image()]),1)

    def test_photo_duplicate_and_maximum_enforced(self):
        trip=self.case.create();self.assertEqual(self.attach(trip,[self.image()]),1);self.assertEqual(self.attach(trip,[self.image()]),0)
        for color in ['green','blue','yellow','black','white']:self.attach(trip,[self.image(color)])
        with self.assertRaises(ValueError):self.attach(trip,[self.image('orange')])
        self.case.db.rollback();self.assertEqual(self.case.db.query(MotoTripPhoto).count(),6)

    def test_non_images_too_large_and_too_many_are_rejected(self):
        for uploads in [[UploadFile(file=BytesIO(b'<html>bad</html>'),filename='recibo.jpg')],[UploadFile(file=BytesIO(b'x'*(photos.MAX_UPLOAD+1)),filename='big.jpg')],[self.image() for _ in range(7)]]:
            with self.assertRaises(ValueError):photos.prepare_uploads(uploads)
        self.assertEqual(self.case.db.query(MotoTripPhoto).count(),0)

    def test_invalid_photo_on_new_trip_never_posts_expense(self):
        request=common.AutoPosTests.request(self.case)
        data=list(self.case.payload().items())+[('csrf',router.csrf(request,self.case.user)),('photos',UploadFile(file=BytesIO(b'bad image'),filename='bad.png'))]
        response=router.create(request,FormData(data),self.case.db,self.case.user)
        self.assertIn('error=',response.headers['location']);self.assertEqual(self.case.db.query(MotoTrip).count(),0);self.assertEqual(self.case.db.query(AccountingEntry).count(),0)

    def test_new_trip_and_photo_saved_in_one_transaction(self):
        request=common.AutoPosTests.request(self.case)
        data=list(self.case.payload().items())+[('csrf',router.csrf(request,self.case.user)),('photos',self.image())]
        response=router.create(request,FormData(data),self.case.db,self.case.user)
        self.assertIn('ok=',response.headers['location']);self.assertEqual(self.case.db.query(MotoTripPhoto).count(),1);self.assertEqual(self.case.db.query(AccountingEntry).count(),1)
        self.assertEqual(self.case.db.query(CajaDiaria).count(),0)
