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


## Interfaz móvil (noviembre 2026)

La vista móvil tiene navegación propia, origen/destino destacados, datos de usuario/fecha compactos y selector de bodega/vendedor plegable. Al preparar una solicitud se priorizan los productos; los filtros de estado reaparecen al cerrar el formulario. La búsqueda distingue existencias locales y disponibles en el origen. Cada producto tiene controles de cantidad entera con +/−, eliminación y aviso cuando supera el saldo consultado.

La barra inferior muestra unidades y permite revisar una solicitud con productos. Una hoja de confirmación presenta bodega, vendedor, productos, cantidades y observaciones antes del envío definitivo. Volver a editar conserva los datos. La barra se oculta cuando el teclado reduce el área visible, y se respetan las áreas seguras del teléfono. Los detalles agrupan correo e historial; un error de correo se despliega para poder reintentarlo. Los cambios son de interfaz: se mantienen los endpoints, permisos y validaciones de inventario.

Pruebas de navegador en anchos 320, 360, 390 y 430 px, más escritorio: cantidades, avisos de saldo, revisión/edición/confirmación, solicitud pendiente, autorización y comprobante. Todos los movimientos de prueba se realizan en SQLite aislado, con correo simulado.
