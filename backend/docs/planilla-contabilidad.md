# Planilla y contabilidad · Pacas Hollywood

Integración local de períodos de planilla y sus pagos. Mantiene los cálculos de salario y deducciones. El control complementario de prestaciones se documenta en `vacaciones-prestaciones.md`.

## Cierre / devengo

El cierre publica un comprobante de diario fechado al término del período, por sucursal. Debe: salario bruto. Haber: neto por pagar y deducciones según sus cuentas. El tiempo no remunerado reduce el gasto salarial. Cada cuota planificada utiliza la cuenta de su tipo de deducción; los ajustes de deducción sin tipo requieren una cuenta explícita. Bruto = neto + deducciones y el desglose debe cuadrar antes del cierre. Neto negativo o cuenta ausente/inactiva bloquea el cierre; cero en todas las líneas no genera un asiento sin contenido.

Se configura gasto salarial, salarios por pagar, deducción manual y cuenta por cada tipo de deducción. Las cuentas estándar del catálogo se proponen inicialmente: salario 6208, obligación 2113, préstamo/anticipo 1108, retención legal 2110 y ausencia como reducción de gasto. Las cuentas son revisables. Los comprobantes ya publicados conservan sus cuentas; el pago utiliza la obligación guardada en el período, aunque luego cambie la configuración.

El control complementario permite provisionar vacaciones, aguinaldo e indemnización estimada. No añade aportes patronales ni recalcula impuestos o contribuciones legales.

## Pago

Con la planilla cerrada, el pago cancela la obligación salarial y acredita la cuenta real de caja o banco seleccionada. No vuelve a debitar gasto. Se paga el neto total del período, como en el flujo de pago existente; no se añadió un pago parcial por empleado.

Efectivo: crea un recibo vinculado al mismo comprobante, actualiza CajaDiaria una vez y entra al cierre por ese recibo. No hay un segundo asiento por el recibo. Bodega y diario deben corresponder a la sucursal. Transferencia/cheque: asiento bancario sin movimiento de caja operativa. Registrar el pago no inicia una transferencia bancaria ni entrega dinero: documenta una ejecución real realizada por el usuario.

## Anulación

Un pago integrado se anula mediante comprobante inverso, no borrado. Reinstala el neto por pagar. Si fue efectivo, crea un recibo de reintegro y restituye caja en la fecha de anulación. El usuario debe haber recuperado/cancelado el pago real antes de esta acción. Se bloquean fechas anteriores al pago, caja/período cerrado y pagos conciliados hasta deshacer su conciliación. Los asientos vinculados no se revierten aisladamente desde contabilidad.

## Historial y protección

Planilla, pago, recibo y asiento se confirman juntos. Bloqueos transaccionales serializan cálculo, cierre, pago, modificaciones de deducciones y cierres de caja. Un período ya vinculado no genera otro devengo; un pago ya vinculado no genera otra salida. Reintentar el formulario de pago no crea otro pago activo.

No se contabilizaron retrospectivamente pagos históricos. Una planilla cerrada y aún sin pago puede generar su devengo al pagar solo si el período contable original está abierto y el desglose es válido. Una planilla histórica ya pagada y sin asiento requiere regularización revisada, sin duplicar gasto ni caja. Las nuevas liquidaciones se integran mediante el control de prestaciones; los registros históricos pagados conservan su estado y no generan movimientos retrospectivos.

## Verificación

Pruebas de desglose, cuadratura, importes inválidos, devengo único, pago de neto, cuentas, recibo/caja una vez, reintegro y pago bancario sin caja. QA en base de datos con todas las escrituras revertidas; no se ejecutaron pagos reales. UI WebKit de configuración y trazabilidad.
