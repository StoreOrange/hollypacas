# PDF del historial de vacaciones · Pacas Holl

En **Vacaciones y prestaciones → Historial de vacaciones**:

- **PDF del movimiento** abre en otra pestaña el comprobante individual del pago, descanso o vacaciones de liquidación.
- **Reporte PDF del historial** exporta los movimientos visibles para la sucursal del administrador y respeta el trabajador seleccionado. El filtro también conserva su selección en pantalla. Se admiten hasta 1,000 movimientos por reporte.

El comprobante individual usa los días, salario y valor originales del movimiento; no recalcula el pago usando el salario actual. Incluye empleado, identificación, sucursal, fecha, estado, motivo, usuario, hora de registro en Nicaragua, recibo de caja cuando exista, comprobante contable, diario y constancia del descuento aplicado a una liquidación borrador. Los movimientos anulados se identifican expresamente y muestran el motivo de reversión. Los descansos se distinguen de los pagos directos.

La forma de registro se determina por los documentos almacenados: efectivo si hay recibo de caja, pago bancario si el diario es de banco, o referencia al comprobante cuando no hay dato suficiente para afirmar un medio más específico.

El reporte de historial suma únicamente pagos directos vigentes y distingue descansos, liquidaciones y anulaciones. Cada endpoint exige sesión de administrador, empresa Pacas Holl y alcance de sucursal. Respuestas PDF de uso privado, sin caché. Generar o repetir un PDF es una operación de consulta: no crea movimientos, pagos ni asientos.

Pruebas: `python -m unittest tests.test_payroll_vacation_pdf`. El generador utiliza ReportLab ya instalado. Las pruebas visuales renderizan con PyMuPDF local, sin añadir dependencias a producción.
