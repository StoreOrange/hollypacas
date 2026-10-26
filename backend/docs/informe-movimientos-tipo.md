# Ingresos y egresos por tipo · Pacas Holl

En Informes aparece «Ingresos y egresos por tipo», ruta `/reports/movimientos-tipo`. Disponible en el entorno Hollpacas con permiso `access.reports`. Solo se consultan sucursales/bodegas activas autorizadas al usuario.

Filtros: desde/hasta (inicio del mes a hoy por defecto), sucursal, una sola bodega, ingresos/egresos/ambos, tipo de ingreso y tipo de egreso. Los tipos salen de la configuración real de inventario: traslados, producción, ajustes y cualquier otro tipo registrado. El cambio de sucursal limita la selección de bodega. Una combinación inválida o no autorizada se rechaza en el servidor.

Muestra costos de ingresos y egresos por separado, resumen por tipo y listado de documentos con fecha, observación, usuario, cantidad, costos C$/US$ y laboratorio. Cada documento cuenta una sola vez; se agregan primero sus líneas para evitar duplicar importes al vincular laboratorios. Los traslados se cuentan únicamente como el ingreso o egreso real de la bodega seleccionada. Las correcciones se presentan con su dirección/tipo documental y la clase del movimiento de laboratorio; no se reinterpretan como ganancia o pérdida.

Los costos parten de los subtotales registrados, o cantidad por costo unitario si falta el subtotal. Para completar una moneda se utiliza solo la tasa del propio documento; no se recalcula al tipo de cambio de hoy ni al costo vigente del catálogo. Si no hay valores ni tasa histórica, el importe de esa moneda queda en cero. Las cantidades son las unidades originales del documento.

La asociación a laboratorio utiliza `production_laboratory_movements`, nunca una coincidencia de observación. Con permiso `access.inventory.egresos`, el número abre la página habitual de laboratorios en una pestaña nueva y su modal directamente, usando la fecha original del laboratorio (aunque el movimiento filtrado sea un ajuste de otra fecha). El acceso mantiene los permisos habituales. Sin ese permiso se informa el número sin enlace.

Verificación: `python -m unittest tests.test_inventory_type_report tests.test_inventory_report_costs tests.test_kardex_consolidated tests.test_shoe_kardex`. Prueba de navegador: `python tests/browser_inventory_types.py`, con páginas reales, SQLite aislada y solicitudes interceptadas; verifica resumen, cambio de sucursal/bodega y apertura del modal de laboratorio. No modifica movimientos ni existencias en producción.
