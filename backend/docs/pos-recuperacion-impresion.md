# Recuperación de impresión nativa de Hollpacas

La impresión automática sigue facturando una sola vez por preventa, después del plazo existente de 15 segundos y respetando el carrito en uso. Cada factura imprime ORIGINAL, COPIA y EXCLUSIVA DE BODEGA en **un único documento y una llamada a window.print()**, en lugar de tres llamadas sucesivas. La copia de bodega conserva su presentación sin precios. No se cambia el flujo manual de impresión ni los PDF existentes.

El ticket espera recursos y responde solo tras el aviso afterprint y el retorno de print(). Se conserva una breve separación antes de liberar el iframe. Si no hay respuesta, el ticket informa UNKNOWN a los 35 segundos, el contenedor tiene un límite de 40 segundos y el servidor identifica DISPATCHED vencidos en los siguientes ciclos. El respaldo no reintenta por sí solo una impresión de resultado desconocido.

En Ventas y en la consola aparece el panel central «Comprobantes pendientes de impresión» al fallar. «Ver pendientes» permite abrirlo antes. «Imprimir todos los pendientes» utiliza las facturas seleccionadas y crea un lote HTML de sus ejemplares existentes; no crea ventas, mueve inventario ni reenvía preventas. El usuario confirma tras revisar impresora/cola de Windows. Puede desmarcar comprobantes ya revisados; las selecciones sobreviven al refresco del estado.

Las rutas backup/claim, backup/ticket y backup/ack verifican usuario, empresa, sucursal y estación. La reserva bloquea la estación y todos los trabajos; rechaza los ya enviados, en curso reciente o fuera de la estación. Un nonce nuevo invalida confirmaciones antiguas. El ACK del lote es idempotente y se conserva localmente si falla la red; un ACK vencido no debe ocultar trabajos que el servidor mantiene sin confirmar.

El navegador no certifica la impresión física: afterprint también puede ocurrir al cerrar la vista previa. Hardware desconectado, papel agotado, controlador o spooler bloqueado requieren revisión local. No se debe marcar SENT por un simple temporizador ni reimprimir automáticamente un resultado desconocido.

Validación aislada:

- `python -m unittest tests.test_pos_browser tests.test_pos_automation tests.test_pos_invoice_copies`
- `python tests/browser_pos_queue.py`: tres preventas FIFO, ventana persistente, popup bloqueado/cerrado y ACK perdido.
- `python -m tests.browser_pos_recovery`: Firefox sin afterprint o con print() rechazado; respaldo de dos facturas/seis ejemplares, confirmación perdida, ausencia de ventas nuevas y selección estable.

Después del despliegue, recargar Ventas y la ventana «Impresión de caja» para obtener pos-browser.js v8. No se modifica Firefox ni se instala software en la caja automáticamente. La salida física debe comprobarse en la Epson real.
