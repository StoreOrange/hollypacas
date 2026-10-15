# Preventas automáticas y POS · solo Pacas Hollywood

Implementación local. No se activó el modo en una caja real ni se generaron facturas/impresiones reales durante QA. La salida física requiere instalar y probar el agente en Windows con la Epson.

## Uso

En Ventas aparece el interruptor «Facturar preventas nuevas e imprimir POS · 40 segundos». Primero descarga «Vincular Epson en Windows», extrae el ZIP y sigue LEEME.txt. Requiere Python 3.9+, SumatraPDF, controlador Epson y Epson como impresora predeterminada del usuario Windows. Descarga el paquete desde el entorno que realmente utilizará caja: un paquete que apunta a localhost solo sirve si ese servidor corre en la misma PC Windows.

El instalador configura una tarea al iniciar sesión Windows, bajo el mismo usuario de la impresora. No requiere puertos de entrada ni acceso desde la VPS a la LAN. El agente hace solicitudes salientes autenticadas, limitadas a los tickets de esa estación. Su credencial puede revocarse desde Ventas. Reinstalar renueva la credencial y reemplaza la tarea de este agente del usuario. No instala ni descarga ejecutables de terceros automáticamente.

Una vez conectado, marcar el checkbox autoriza facturar nuevas preventas como CONTADO, EFECTIVO, C$ por el total calculado. El temporizador comienza cuando la caja recibe la preventa en su cola (consulta cada dos segundos). Solo pendientes no congeladas de la bodega y del día actual, con ID posterior a la activación. No factura pendientes históricos. Si una preventa pasa a revisión manual, se cancela, se congela o ya fue facturada, se retira de la automatización. Puede omitirse individualmente durante la cuenta regresiva; sigue disponible manualmente.

Al vencer el plazo, usa el mismo registro de ventas para calcular importes, productos libreados/combinaciones, inventario, cobro y contabilidad. La última factura se presenta con cliente, detalle, total y enlace al PDF POS en Ventas. No se inyecta una factura ya pagada en el carrito editable. Mientras el carrito contiene productos, una preventa manual o un modal de venta abierto, no se genera la factura automática. Se mantiene el trabajo manual y se retoma después de limpiar/terminar.

Solo una estación con conexión reciente puede controlar la bodega. Una estación abandonada puede sustituirse después de 25 segundos; sus esperas se cancelan. El checkbox se conserva por usuario/navegador/bodega, no activa otras empresas. Ventas debe permanecer abierta, la PC encendida, usuario Windows conectado y agente operativo. Los navegadores pueden retrasar temporizadores en segundo plano/suspensión: no se garantiza una ejecución exacta al segundo bajo esas condiciones. Cuando el agente se desconecta, pausa la facturación; retoma al reconectarse. Un cierre de caja/contable o falta de inventario deja error visible y revisión manual, sin reintentos de facturación ciegos.

## Consistencia

Bloqueo financiero y fila de bodega serializan estaciones, facturas manuales y activaciones. Cada preventa tiene una tarea única, con plazo persistido y clave de operación estable. Factura, pago, estado FACTURADA de preventa y trabajo POS se confirman en la misma transacción. Si falla la venta, no queda una impresión huérfana; si se pierde la respuesta después de confirmar, no genera otra venta. El flujo automático suprime la impresión antigua del servidor y no navega a la página que abre el diálogo del navegador; así evita dobles impresiones.

El agente pide un trabajo QUEUED. El servidor prepara el PDF POS existente, sus copias configuradas (1–5), un identificador de intento y checksum, y lo marca DISPATCHED antes de entregarlo. Nunca lo entrega automáticamente a otro intento. El agente guarda su estado local antes de ejecutar SumatraPDF en modo silencioso con la Epson predeterminada. Si el envío termina, reporta SENT; si se interrumpe o su resultado es incierto, UNKNOWN. Una respuesta de ACK perdida se reintenta sin volver a enviar al spooler. Un intento obsoleto no bloquea un reintento nuevo solicitado manualmente.

SENT acredita entrega al sistema de impresión Windows, **no** certifica papel impreso. Atascos, falta de papel, controlador u offline pueden conocerse solo en Windows/impresora. Si falla o no hay confirmación después de dos minutos, aparece «Reimprimir misma factura», con confirmación para revisar antes la cola. Esto cambia el intento del mismo trabajo y no factura otra vez. Un trabajo confirmado SENT no se reenvía automáticamente.

La automatización factura exclusivamente como efectivo C$ por el total, según lo autorizado. No acredita transferencias, tarjetas ni crédito, ni verifica una entrega física de dinero. La activación corresponde al procedimiento operativo real de la caja.

## Componentes / instalación

- Modelos nuevos aditivos: pos_auto_stations, pos_auto_tasks, pos_print_jobs. Se crean con el arranque habitual; no modifican documentos históricos.
- Rutas de caja bajo /sales/auto-preventas requieren usuario vigente, permiso de registro y sucursal/bodega de la estación. Rutas de agente bajo /api/pos-agent requieren credencial aleatoria con hash almacenado y usuario activo con acceso a ventas/sucursal.
- scripts/hollpacas_pos_agent.py: biblioteca estándar, HTTPS validado, sin redirecciones ni shell ni comandos recibidos desde el servidor. Solo un proceso por perfil local; estado y ACK duraderos.
- deploy/install_pos_agent_windows.ps1: tarea por usuario Windows, configuración privada bajo LOCALAPPDATA/HollpacasPOS.
- La impresora predeterminada se resuelve en Windows, no se confunde con la impresora del servidor VPS. El formato es el PDF POS existente y las copias de la configuración por sucursal.

## Verificación

Pruebas SQLite aisladas ejecutan el registro de ventas real con efectivo C$, producto libreado, inventario y asientos; cubren 40 segundos, repetición, caja cerrada, error por stock, revisión/congelación, exclusión de antiguas, pausa por actividad/agente, estación única, credencial/revocación, ZIP, cola, ACK y reimpresión. Pruebas de agente simulan el comando Windows y los fallos; no ejecutan Sumatra ni imprimen. WebKit verifica el checkbox y la cola con APIs simuladas. La instalación/tarea Windows y la impresión física Epson quedan pendientes de prueba en la PC de caja.

Documentación de impresión silenciosa y límites del resultado de spooler: https://www.sumatrapdfreader.org/docs/Command-line-arguments .
