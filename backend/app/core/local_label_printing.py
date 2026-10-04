"""Local CUPS printing for the Miss Zapatos test environment."""
import os
import re
import subprocess
import tempfile
from urllib.parse import urlsplit

from fastapi import HTTPException


def require_local(request):
    if not request.client or request.client.host not in {'127.0.0.1', '::1'}:
        raise HTTPException(403, 'La impresión directa requiere abrir el sistema en la computadora de la impresora.')
    origin = request.headers.get('origin')
    if origin and urlsplit(origin).netloc != request.headers.get('host'):
        raise HTTPException(403, 'Origen de impresión no permitido')
    if request.headers.get('x-requested-with') != 'fetch':
        raise HTTPException(403, 'Solicitud de impresión no permitida')


def _run(args):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=20,
                                env={**os.environ, 'LC_ALL': 'C', 'LANG': 'C'})
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(503, 'El servicio de impresión no respondió. Revise la cola antes de reintentar.') from exc
    except OSError as exc:
        raise HTTPException(503, 'No está disponible el servicio local de impresión CUPS.') from exc
    if result.returncode:
        raise HTTPException(503, 'No se pudo completar la operación. Revise la conexión y la cola de la impresora.')
    return result.stdout


def printers():
    output = _run(['lpstat', '-p'])
    return [{'id': m.group(1), 'name': m.group(1).replace('_', ' '),
             'enabled': not any(word in m.group(2).lower() for word in ('disabled', 'deshabilitada', 'desactivada'))}
            for m in re.finditer(r'^(?:printer|la impresora) (\S+) (.*)$', output, re.M)]


def submit_pdf(content, printer, width_mm=50, height_mm=20):
    available = {p['id']: p for p in printers()}
    if printer not in available or not available[printer]['enabled']:
        raise HTTPException(400, 'Seleccione una impresora disponible y habilitada.')
    with tempfile.NamedTemporaryFile(suffix='.pdf') as document:
        document.write(content)
        document.flush()
        output = _run(['lp', '-d', printer, '-t', 'Miss Zapatos - Etiquetas',
                       '-n', '1', '-o', f'PageSize=Custom.{width_mm:g}x{height_mm:g}mm', '-o', 'print-scaling=none',
                       '-o', 'scaling=100', '-o', 'orientation-requested=3', document.name])
    match = re.search(r'request id is (\S+)', output)
    return match.group(1) if match else 'aceptado'
