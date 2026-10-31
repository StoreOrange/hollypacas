# Cartera y abonos móviles · Holl Pacas

Acceso en el inicio móvil, menú móvil y pantalla de preventa: **Cartera y abonos**, `/m/cartera`. Interfaz adaptable de una columna. El acceso del menú se muestra tanto en móvil como en pantalla grande para abrir la misma interfaz de cobranza; queda junto a Nueva preventa incluso con órdenes de menú anteriores.

- Vendedor vinculado mediante `users.vendedor_id`: consulta, registra y adjunta soporte solo en sus facturas. No se infiere la identidad por nombre ni se acepta un vendedor enviado en el formulario. Por indicación del propietario, los vendedores sin vinculación no ven el acceso ni pueden registrar abonos. No se crean ni modifican asignaciones automáticamente.
- Administrador: cartera completa de las sucursales de la empresa, resumen por vendedor, filtros por cliente/factura, pendientes/saldadas y consulta de abonos por fecha, vendedor, recibo o referencia bancaria. Listados de 50 registros por página.
- Las facturas de Holl marcadas expresamente **PENDIENTE** pueden conservar un registro completo de forma de pago de la emisión. Ese registro no representa dinero recibido en la cobranza. Se excluye del saldo y, al registrar el primer abono móvil, se conserva la identificación de esas filas en `mobile_collection_bases`, para que al liquidar no se duplique el importe pagado. No se borran ni modifican los pagos originales. Los pagos iniciales parciales reales se mantienen y las facturas ya pagadas no se reabren.
- Saldo nativo de factura: total menos pagos iniciales y abonos, incluyendo los signos de las notas de crédito/débito. Se incluyen las facturas con saldo de cualquier condición, como la cobranza actual de Holl; las anuladas no entran en la cartera cobrable.
- Pago parcial o completo en C$/USD: tasa registrada a la fecha del pago, monto positivo con dos decimales, sin sobreabono incluso al convertir moneda. No admite fecha futura ni anterior a la factura.
- Depósito/transferencia: banco, referencia e imagen obligatorios. Efectivo: imagen opcional. Se crea un `CobranzaAbono` normal; en Holl mantiene `afecta_caja=False`, igual que la cobranza existente. No crea recibos de caja ni asientos bancarios adicionales.
- Comprobante e historial: el pago queda junto a la factura y reduce la cartera existente. Las imágenes se consultan con autorización, en un visor móvil, mediante `/m/cartera/soportes/{id}`.

## Integridad y almacenamiento

Nueva tabla `mobile_collection_payments`: vincula un abono normal, usuario, vendedor, método, referencia, banco e imagen JPEG privada. El arranque habitual (`init_db`) crea la tabla. No migra ni recalcula pagos anteriores. No se almacenan adjuntos bajo `/static` ni se conserva el nombre original. JPG/PNG/WebP de hasta 8 MB y 25 MP, normalizados y con metadatos eliminados.

La solicitud lleva un identificador firmado asociado al usuario y factura. Un reintento devuelve el mismo resultado. Una referencia bancaria repetida en la misma factura no crea un segundo pago. Escrituras serializadas mediante bloqueo de factura y bodega; los endpoints existentes de abonos comparten esos bloqueos. El abono y su soporte se confirman en la misma transacción. Se conserva el historial móvil con una respuesta clara si se intenta eliminar el abono desde el control anterior, en lugar de romper su vínculo de auditoría.

## Verificación y estado

`python -m unittest tests.test_mobile_collections` prueba propiedad de facturas/soportes, pagos iniciales/notas, ambas monedas, liquidación, sobreabonos, reintentos, referencias repetidas, imágenes inválidas, fechas, fallos transaccionales y borrado desde la ruta anterior. Usa exclusivamente SQLite aislado.

`python tests/browser_mobile_collections.py` comprueba interfaz móvil de vendedor y administrador, pago parcial/completo, foto, visor, historial y ancho adaptable con la misma base aislada. Los pagos de prueba no se envían a la base del negocio.

El menú «Revisión de deudas» abre `/m/deudas`: el administrador consulta la cartera completa y el vendedor únicamente la suya. El cierre masivo autorizado se ejecuta como operación administrativa independiente, con respaldo y excepciones explícitas; genera ajustes de cartera sin movimiento de caja y conserva las ventas y sus registros originales.
