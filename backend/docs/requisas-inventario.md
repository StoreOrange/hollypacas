# Requisas de inventario · Pacas Holl

Acceso: **Ventas → Requisas de inventario**, menú lateral o acceso del inicio móvil. URL: `/sales/requisas`. Exclusivo de la empresa `hollywood_pacas` y las bodegas principales Central y Estelí.

## Operación

- El usuario de caja/vendedor solicita para su propia bodega; un administrador puede elegir Central o Estelí. La bodega de origen es la otra.
- La fecha y usuario se toman del servidor con hora de Nicaragua. El vendedor se elige entre los asignados a esa bodega; el primero aparece seleccionado.
- Búsqueda por código y palabras del nombre, con tolerancia a tildes; muestra existencias del kardex en ambas bodegas. Cantidades enteras positivas, hasta 60 productos. Observaciones opcionales.
- `pending`: pendiente de autorización, sin reserva ni movimiento de inventario.
- `active`: autorizada y trasladada. Se crean un egreso del origen y un ingreso del destino; los documentos se enlazan mediante `Egreso #...`, compatibles con kardex y sus PDF.
- `cancelled`: anulada, sin traslado. No puede reactivarse: registrar una nueva solicitud.
- Solo un administrador autoriza. El solicitante o administrador pueden anular una pendiente, indicando el motivo. Los demás usuarios de la misma bodega pueden consultar.
- Al autorizar se revisan nuevamente todos los saldos. Si uno no alcanza o un producto está desactivado, se anula toda la solicitud con el motivo. No existen traslados parciales.
- En la pantalla de saldo insuficiente no se permite autorizar; se ofrece anular y preparar otra solicitud. Si el saldo cambia entre revisión y autorización, el servidor vuelve a validarlo.

## Inventario y trazabilidad

La aprobación bloquea la solicitud y las dos bodegas en orden de ID dentro de la misma transacción. Las ventas ya bloquean la bodega; los egresos ordinarios de Holl ahora usan el mismo bloqueo antes de consultar saldo. Dos autorizaciones simultáneas no duplican documentos ni consumen el mismo saldo. Las cantidades del origen y destino son idénticas; el saldo auxiliar global del producto no cambia. No se alteran precios ni costos del producto.

Los movimientos usan la fecha del servidor al autorizar. Se conservan por separado fecha/hora de solicitud, decisión, identificadores y nombres de usuarios, vendedor, productos y cantidades, observaciones, motivo de anulación y números de egreso/ingreso. Una solicitud resuelta no se edita.

## Correo

Tras confirmar la solicitud se programa el envío a los destinatarios actualmente activos de `email_recipients`, usando el emisor de `email_config` y SMTP existentes. El mensaje detalla productos, cantidades, solicitante y ruta, con enlace de solo lectura **Revisar y autorizar**. Abrirlo no autoriza ni mueve inventario; exige sesión y la autorización exige rol administrador y POST firmado. Si es necesario iniciar sesión, se retorna a esa solicitud.

La solicitud se conserva aunque falle el correo. El detalle muestra estado y error, y permite reintentar al solicitante o administrador. Un envío registrado como exitoso no se vuelve a enviar. Si el proceso se interrumpe durante el envío, se puede reintentar después de 5 minutos; como en cualquier SMTP, una interrupción justo después de aceptar el mensaje puede producir un aviso duplicado, nunca un traslado duplicado.

## Comprobantes

Solo una solicitud activa permite generar recibo POS de 80 mm desde el navegador y JPG para compartir/descargar. Incluyen ruta, cantidades, actores, hora de Nicaragua y documentos. El comprobante certifica el traslado en el sistema; la firma física de entrega/recepción puede completarse en el recibo.

## Despliegue y pruebas

Las tres tablas nuevas son `inventory_requests`, `inventory_request_items` e `inventory_request_events`. Se registran en el arranque habitual con `Base.metadata.create_all`. Para crearlas de forma explícita antes del reinicio, desde `backend`: `PYTHONPATH=. venv/bin/python scripts/create_inventory_requests.py`. Este script solo crea esas tablas y no modifica inventario.

Pruebas unitarias: `python -m unittest tests.test_inventory_requests`.
UI aislada: `python tests/browser_inventory_requests.py` (Firefox móvil y escritorio).
Concurrencia: `python tests/postgres_inventory_requests.py`, exclusivamente en PostgreSQL local con esquemas temporales eliminados al terminar; nunca ejecutar pruebas de escritura contra inventario productivo.
