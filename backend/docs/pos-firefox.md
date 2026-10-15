# Auto POS · 40 s (Hollpacas)

Interruptor compacto junto a Ver preventas, sin panel ni agente. Se conserva por usuario/navegador/bodega; inicialmente apagado. Las antiguas rutas del agente permanecen deshabilitadas.

Requiere Firefox en Windows con la Epson/controlador/papel POS seleccionados y `print.always_print_silent=true` en about:config. Esa preferencia se configura una vez en la PC; una web no puede activarla ni comprobarla. Primero comprobar una impresión POS normal con la Epson. No requiere Python, SumatraPDF ni instalación del agente. Mantener Ventas abierta y activa.

Al marcar, toma únicamente preventas nuevas pendientes, no congeladas y de la bodega/día actual. No procesa retrospectivamente las pendientes al activar. Cuenta 40 segundos desde su recepción en la cola (consulta cada 2 segundos). Genera la factura con el mismo flujo de ventas: efectivo C$ por el total, inventario, libreados y contabilidad. Se pausa mientras existe una venta/preventa manual o modal abierto. Desmarcar cancela las esperas aún no facturadas.

Se reutilizan las tablas aditivas de tareas/facturas y sus claves estables para impedir facturas duplicadas, tanto al reintentar como entre pestañas/cajas. Solo una estación reciente controla cada bodega. La factura y su trabajo POS se confirman juntos. No usa el ejecutable del servidor ni abre ventanas nuevas: carga el ticket POS existente en un iframe de la misma web y solicita impresión nativa. El último número queda visible con enlace a la factura. Respeta copias por sucursal (1–5).

Cada trabajo se entrega una sola vez; el navegador registra el intento antes de imprimir. Al finalizar los eventos afterprint de las copias, confirma al servidor; si se pierde la confirmación, la guarda localmente y reintenta solo el ACK. No vuelve a facturar ni repite automáticamente una impresión incierta. No se generan nuevas facturas mientras exista un trabajo en cola, en impresión o sin confirmación. Sin evento tras 90 segundos se muestra Revisar Epson; si se cerró la pestaña antes, permite revisar/reintentar tras 120 segundos. El botón de impresora reimprime la misma factura después de confirmación manual.

`afterprint` no certifica salida física y puede ocurrir tras cancelar un diálogo si Firefox no está configurado para impresión silenciosa. La web no identifica papel, atascos o impresora apagada. Firefox/Windows en suspensión o pestañas en segundo plano pueden retrasar el temporizador; mantener caja activa. La preferencia silenciosa afecta al perfil del navegador, no solo a Hollpacas.

QA: pruebas de servicios con facturas/asientos reales en SQLite aislada, plazo, pausa, idempotencia, entrega/ACK/reimpresión, ticket y ausencia de rutas de agente. Firefox automatizado valida el interruptor pequeño, iframe POS, copias/evento/ACK y ausencia de repetición, simulando la API y window.print; no se generaron ventas ni impresiones reales. La prueba física Epson debe realizarse en la PC Windows.

Referencia técnica Mozilla: https://github.com/mozilla-firefox/firefox/blob/main/modules/libpref/init/StaticPrefList.yaml (`print.always_print_silent`).

## Recuperación de conexión

La consulta tiene tiempo máximo de 8 segundos. Los temporizadores se inician antes de la primera conexión, para que un fallo inicial no impida volver a consultar. Reintenta automáticamente y al regresar a la pestaña; muestra Reconectando mientras no recibe datos. El contador visible usa una fecha límite local que no se extiende con cada respuesta del servidor. Al estar apagado se indica Apagado. El texto junto a Auto POS muestra el contador real cuando hay una preventa pendiente. QA de regresión: `python tests/browser_pos_timer_recovery.py` con app local y Firefox de Playwright; APIs simuladas, sin facturas ni impresiones reales.
