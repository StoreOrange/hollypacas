# Auto POS · 15 s (Hollpacas)

Requiere Firefox en Windows con la Epson/controlador/papel POS seleccionados y `print.always_print_silent=true` en about:config. Esa preferencia se configura una vez en la PC; una web no puede activarla ni comprobarla. Primero comprobar una impresión POS normal con la Epson. No requiere instalar el agente Windows.

Al activar Auto POS en Ventas se abre la ventana «Impresión de caja». Permitir ventanas emergentes para Hollpacas y mantener esa ventana abierta. Se puede navegar a Inventario, Preventas u otras pantallas en la ventana principal: el procesamiento sigue en la ventana de impresión. «Abrir impresión» recupera la ventana existente; no activa por sí solo el modo automático. Si el modo ya estaba activado al entrar a Ventas, usar ese botón. Si se cierra la ventana de impresión, Ventas muestra que debe abrirse nuevamente. Una única ventana por estación controla la cola mediante Web Locks; requiere contexto seguro HTTPS (o localhost).

Al marcar, toma únicamente preventas nuevas pendientes, no congeladas y de la bodega/día actual. No procesa retrospectivamente las pendientes al activar. Cuenta 15 segundos desde su recepción en la cola (consulta cada 2 segundos). Genera la factura mediante el mismo flujo de ventas: efectivo C$ por el total, inventario, libreados y contabilidad. Se pausa cuando la ventana principal de Ventas informa un carrito/preventa manual o modal abierto. Desmarcar cancela las esperas aún no facturadas.

Las preventas se ordenan por recepción y se procesa una a la vez. Factura y trabajo POS se confirman juntos. No se factura la siguiente hasta confirmar la impresión anterior. La entrega toma el trabajo pendiente más antiguo; un trabajo incierto impide adelantar otro. Todos los trabajos de impresión pendientes permanecen visibles aunque haya más de diez tareas posteriores. La comprobación de pendientes se repite bajo el bloqueo de facturación para evitar carreras entre solicitudes concurrentes.

Se carga el ticket POS existente en un iframe dentro de la ventana de impresión y se respeta la configuración de copias (1–5). La preparación del ticket no depende de requestAnimationFrame, que se suspende en pestañas sin foco. El navegador registra cada intento antes de imprimir. Al finalizar los eventos afterprint confirma al servidor; si se pierde la confirmación, guarda localmente y reintenta solo el ACK. No vuelve a facturar ni repite automáticamente una impresión incierta. Una segunda ventana no puede controlar simultáneamente la misma estación.

Sin evento tras 90 segundos se muestra Revisar Epson; si ni siquiera carga el ticket, la ventana también detecta la ausencia de respuesta a los 100 segundos. Si se cerró la ventana durante una impresión, permite revisar/reintentar tras 120 segundos. El botón de impresora en la ventana de impresión reimprime la misma factura después de confirmación manual. Revisar antes la Epson y su cola para evitar duplicar papel.

`afterprint` no certifica salida física y puede ocurrir tras cancelar un diálogo si Firefox no está configurado para impresión silenciosa. La web no identifica papel, atascos ni impresora apagada. Suspender la PC o cerrar el navegador detiene el procesamiento; los navegadores pueden retrasar temporizadores en segundo plano. Las preferencias de impresión silenciosa afectan al perfil del navegador.

## Verificación local

` .venv/bin/python -m unittest tests.test_pos_browser tests.test_pos_automation ` ejecuta facturación real en SQLite aislada: plazo, pausa, idempotencia, cola FIFO de varias preventas, entrega/ACK/reimpresión, trabajo pendiente fuera del historial, bloqueo por impresión incierta, permisos y ausencia de rutas antiguas del agente.

` .venv/bin/python tests/browser_pos_queue.py ` usa Firefox de Playwright y APIs simuladas. Verifica navegar fuera de Ventas, imprimir tres tickets en orden, perder un ACK sin reenviar el ticket y excluir una segunda ventana. No genera facturas ni impresiones reales. `browser_pos_timer_recovery.py` conserva el punto de entrada anterior y ejecuta esta regresión.

Las consultas de recepción y entrega/confirmación bloquean únicamente la fila de estación. La facturación conserva la serialización por bodega y las claves contra duplicados. El estado usa hasta cuatro consultas sin cargar productos de facturas históricas. Las rutas de base de datos usan el pool de hilos. La salida física Epson debe verificarse en la PC Windows.
