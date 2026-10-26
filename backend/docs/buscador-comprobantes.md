# Buscador de comprobantes · Holl Pacas

Ruta `/accounting/comprobantes`, accesible desde el panel contable, su navegación y el formulario de comprobantes. Requiere `access.accounting.entries` y conserva las sucursales autorizadas. El historial anterior sigue disponible, pero este buscador no queda restringido a sus 120 resultados ni a los últimos siete días: pagina de 50 en 50, con conteo total.

Filtros: número, referencia/documento soporte, concepto y detalle de líneas; número de factura asociado a AUTO-VTA/AUTO-COSTO-VTA; recibo vinculado al asiento; fechas, mes, sucursal, diario, tipo, estado, cuenta y monto. El monto exacto compara totales o líneas; desde/hasta compara el total Debe. El selector monetario busca C$ de libros o importe de captura usando la tasa histórica del asiento (sin tasa no se infiere USD). No busca OCR ni documentos que no se hayan registrado en el sistema.

Cada resultado ofrece PDF, detalle de cuentas y contacto, información de registro/publicación/reversión y, cuando existe un vínculo verificable, enlace a la factura o recibo de origen.

Los borradores se corrigen en el editor normal. «Corregir» en un publicado manual exige motivo y fecha; aplica todas las restricciones de reversión existentes, conserva el original y sus líneas, crea la contrapartida publicada y un reemplazo BORRADOR con cuentas/importes editables. Todo ocurre en una transacción; el borrador no afecta libros hasta validarse y publicarse. Se registran actor, fecha, motivo y número del reemplazo en el original. El reintento inmediato recupera el borrador sin duplicar reversión.

Se respetan conciliaciones, cierres y restricciones de caja/planilla. No se corrigen automáticamente por esta vía asientos de operaciones AUTO-; deben coordinarse con el documento de origen. No se modifican comprobantes reales al desplegar. No se agregan tablas ni migraciones.

Pruebas: `python -m unittest tests.test_accounting_registry tests.test_accounting_workflow tests.test_inventory_type_report tests.test_accounting_periods tests.test_accounting_reconciliation tests.test_accounting_cash_receipts`. QA visual y de flujo aislado: `python tests/browser_accounting_registry.py` y `python tests/browser_inventory_types.py`. Las pruebas usan SQLite, nunca la contabilidad de producción.

El informe de movimientos por tipo ahora muestra debajo de cada documento su código y descripción de producto, cantidad, costo unitario y subtotales C$/US$, obtenidos del mismo movimiento con carga agrupada. Se conservan los totales por documento/tipo y los enlaces de laboratorio.
