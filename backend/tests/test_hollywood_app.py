import json
import unittest
from pathlib import Path
from unittest.mock import patch
from PIL import Image
from starlette.requests import Request
from fastapi import HTTPException
from app.routers import web as w


class HollywoodAppTests(unittest.TestCase):
    def request(self, company):
        request = Request({'type': 'http', 'headers': []})
        request.state.branding = {'company_key': company, 'trade_name': 'Mi negocio'}
        return request

    def test_holl_name_home_and_stable_installation_id(self):
        response = w.mobile_preventas_manifest(self.request('hollywood_pacas'), None)
        manifest = json.loads(response.body)
        self.assertEqual(manifest['name'], 'Hollywood App')
        self.assertEqual(manifest['short_name'], 'Hollywood App')
        self.assertEqual(manifest['start_url'], '/')
        self.assertEqual(manifest['id'], '/m/preventas')
        self.assertEqual(manifest['display'], 'standalone')
        self.assertEqual(response.media_type, 'application/manifest+json')
        root = Path(__file__).resolve().parents[1] / 'app'
        for icon in manifest['icons']:
            with Image.open(root / icon['src'].lstrip('/')) as image:
                self.assertEqual(f'{image.width}x{image.height}', icon['sizes'])

    def test_other_companies_keep_preventas_name_and_launch(self):
        manifest = json.loads(w.mobile_preventas_manifest(self.request('bdzapatos'), None).body)
        self.assertEqual(manifest['short_name'], 'Preventas')
        self.assertEqual(manifest['start_url'], '/m/preventas')

    def test_android_domain_association_is_scoped_and_public_certificate_only(self):
        with patch.object(w, 'get_active_company_key', return_value='hollywood_pacas'):
            response = w.hollywood_android_assetlinks()
        data = json.loads(Path(response.path).read_text())
        self.assertEqual(data[0]['target']['package_name'], 'ni.hollywoodpacas.app')
        self.assertRegex(data[0]['target']['sha256_cert_fingerprints'][0], r'^(?:[A-F0-9]{2}:){31}[A-F0-9]{2}$')
        with patch.object(w, 'get_active_company_key', return_value='bdzapatos'):
            with self.assertRaises(HTTPException) as raised:
                w.hollywood_android_assetlinks()
        self.assertEqual(raised.exception.status_code, 404)
