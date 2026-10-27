# Gastos asistidos de Holl Pacas

Ruta `/m/gastos`, exclusiva de Holl Pacas y permiso `access.accounting.mobile_expenses`. Administradores lo tienen por su acceso actual; para otros responsables se asigna el permiso desde la gestión existente. Botón en el inicio móvil y accesos en preventas, pre-factura y menú. La nueva cabecera móvil permite buscar “registrar gasto”.

Se solicita motivo, tipo, monto, moneda, fecha, sucursal, origen real del dinero y beneficiario. Factura/referencia o soporte pendiente explícito. El botón de sugerencia usa coincidencias locales por motivo; el responsable confirma la categoría. Antes de enviar se muestra el resumen, y el servidor elige las cuentas desde un catálogo controlado. No acepta cuentas enviadas por el cliente, no usa grupos padres ni cae en cuentas de activo/inventario si falta configuración. Incluye gastos extraordinarios e imprevistos en 6901 Otros Gastos.

- Caja: diario CASH y cuenta de caja activa de la sucursal; bodega válida y caja abierta. Se crea un ROC, un asiento vinculado y un único descuento en CajaDiaria, por lo que el cierre operativo incluye el gasto.
- Banco: diario BANK y cuenta bancaria activa; un asiento, sin ROC ni movimiento de caja de ventas.
- Dinero personal: se identifica quién pagó; gasto contra 2109 Gastos Acumulados por Pagar, sin tocar caja o banco. El reembolso posterior se tramita en contabilidad y no debe volver a reconocerse como gasto.

Es un registro explícito en libros, aun con la contabilización automática general desactivada. Respeta sucursal, moneda, precisión, tasa histórica a la fecha del gasto y períodos cerrados; bloquea fechas futuras y conversiones fuera del rango. Importe original y moneda se conservan además del equivalente contable en C$.

Toda la transacción es atómica. El bloqueo financiero compartido serializa en PostgreSQL y la misma clave de operación es única en gasto/asiento/ROC; reintentos devuelven el registro existente sin volver a descontar caja. El ROC de estos gastos no se puede eliminar desde el botón antiguo de anulación; se conserva el soporte contable. Los registros históricos no se reclasifican ni modifican.

Después de guardar se abre el recibo PDF carta con logo, beneficiario, motivo, tipo, monto original, soporte, origen, quién pagó personalmente, referencias del ROC/asiento, cuadre, auditoría y firmas. Usa el visor local de PDF ya existente, con imprimir/abrir/registrar siguiente y listado de los últimos 20 gastos de sucursales autorizadas. Su PDF tiene su propio permiso y vuelve a validar el alcance de sucursal.

El modelo nuevo es `mobile_expenses`. `init_db` lo registra con el esquema existente. Para una instalación controlada, `scripts/configure_mobile_expenses.py` previsualiza y, con `--apply --audit-file <archivo-nuevo>`, crea exclusivamente esa tabla y el permiso; verifica empresa y deja constancia previa. No crea cuentas, asientos ni saldos. El despliegue autorizado el 27 de octubre de 2026 incluye respaldo previo y ejecución controlada de este comando en la VPS. Las pruebas de guardado se realizan en bases aisladas.

QA: 16 pruebas de gasto (caja/banco/reembolso, moneda/tasa, doble envío, alcance, permiso real, cierre, validaciones, rollback y PDF), pruebas contables previas y navegación móvil con datos SQLite sintéticos. Se verifican recibo, impresión simulada, ancho 390px y continuidad; no se imprimen papeles ni se registran gastos en producción.

Corrección de entorno local: una versión inicial había creado la tabla antes de añadir amount/currency/exchange_rate. Se agregó migración incremental idempotente, también ejecutada al iniciar Holl Pacas. Recupera esos metadatos desde asientos/ROC si hay filas antiguas, sin modificar libros ni caja; en la base local afectada había cero gastos. La caja configurada en el diario vuelve a estar disponible aunque tenga subcuentas, conservando las validaciones de activo, naturaleza, tipo y código de caja/banco. El comando de configuración admite --local-only para rechazar hosts remotos.

Verificación de la corrección: `tests/verify_mobile_expenses_local.py` exige host local y usa tablas temporales para probar migración y datos inválidos. Prueba los handlers HTTP con la configuración real dentro de una transacción exterior que siempre revierte, incluso cuando la aplicación confirma el guardado; confirma conteos de gasto/asiento/ROC antes y después. Requiere httpx como dependencia de pruebas. Se comprobó además el clic real Home → Gastos en navegador a 390 px, sin simular la página ni permitir solicitudes de escritura.

Ayuda visual: campos y acciones incorporan iconos Bootstrap. El icono de categoría cambia con el tipo seleccionado (combustible, reparación, transporte, etc.), y el del pago distingue caja, banco y dinero personal. Moneda, revisión, guardado, historial y visor conservan sus iconos durante los cambios de estado. Solo presentación; las validaciones y cuentas no cambian.
