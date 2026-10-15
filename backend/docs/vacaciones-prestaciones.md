# Vacaciones y prestaciones · Pacas Hollywood

Disponible en `/payroll/vacations`, desde Planilla → Vacaciones y prestaciones. Implementación local; no se ha enviado a producción.

## Saldo laboral

Acumula 2.5 días por mes calendario completo, con prorrateo diario por los días reales de cada mes. Los descansos son días calendario continuos, incluyendo fines de semana. Saldo = acumulado desde ingreso o fecha histórica pagada + 1 − movimientos activos. El salario ordinario mensual fijo / 30 determina el valor. Los campos históricos `vacation_paid_through`, `bonus_paid_through` y `seniority_paid_through` se conservan. Es necesario revisar que reflejen los pagos anteriores antes de incorporar los saldos: el sistema no puede reconstruir vacaciones disfrutadas que nunca se registraron.

Descanso: reserva los días inmediatamente; bloquea fechas pasadas, rangos inválidos, superposiciones y planillas cerradas. El sueldo ordinario sigue en la planilla habitual. Las marcaciones durante ese descanso no generan tardanzas ni suplementos de feriado/horas extra. No genera un egreso adicional de caja. Al cerrar cada período se debita vacaciones por pagar y acredita gasto salarial por su porción de descanso para evitar reconocer dos veces el gasto provisionado.

Pago directo autorizado: cancela el pasivo de vacaciones contra caja/banco. Efectivo genera recibo y afecta CajaDiaria una vez; banco no modifica caja operativa. El recibo comparte el comprobante; no genera otro asiento. No ejecuta una transferencia bancaria. Motivo/documento de autorización obligatorio. No se permiten días negativos, saldo insuficiente ni repetición del formulario. El pago requiere documentación y revisión de su procedencia; no convierte el derecho al descanso remunerado en una sustitución automática por dinero.

Correcciones: un descanso futuro puede cancelarse antes de comenzar y sin planillas aplicadas. Un pago se anula con asiento inverso y reintegro documentado (efectivo: recibo de ingreso), sin borrar historial. Pagos conciliados requieren deshacer la conciliación. No se anulan aquí liquidaciones finales ni vacaciones de trabajadores liquidados. Reintegro/anulación supone que se canceló o recuperó el pago real.

## Incorporación y provisión diaria

La configuración requiere gasto de prestaciones, tres cuentas separadas de pasivo (vacaciones, aguinaldo, indemnización) y contrapartida del saldo inicial. La primera incorporación publica el acumulado actual en fecha de inicio, sin reabrir/recrear asientos históricos. La revisión expresa del saldo inicial evita reconocer por segunda vez pasivos ya contabilizados. Si ya existen provisiones antiguas, su reclasificación debe cuadrar contra ellas: elegir gasto otra vez duplicaría el reconocimiento. No se activó la configuración ni se crearon movimientos reales durante la implementación.

Después de activar, un trabajador de fondo comprueba cada minuto los días pendientes, aplica las diferencias entre obligación esperada y última provisión, registra instantánea diaria y recupera pendientes al reiniciar. Los cambios de salario revalúan el saldo restante. No contabiliza días futuros. Fechas/cajas cerradas, cuentas inactivas, salario o ingreso ausentes dejan error visible y reintento; nunca omite silenciosamente la causa. La configuración se mantiene después del primer asiento, para impedir cambios de cuenta que rompan la conciliación.

Se serializa con el mismo bloqueo financiero de cierre contable/planilla/caja y se confirma cada operación en una sola transacción. El calendario laboral y los valores calculados están separados del pasivo: reservar descanso no cancela el pasivo hasta su planilla. El pago directo sí lo cancela. Los comprobantes de prestaciones y sus reversas no se revierten aisladamente desde contabilidad.

Aguinaldo: el saldo no desaparece por cambiar de diciembre a enero; conserva lo pendiente desde ingreso o último pago histórico documentado. No se autorizan acumulaciones voluntarias ni se implementó aquí un flujo de pago anual de aguinaldo ni multas por mora. La organización debe cumplir sus fechas de pago; esta reserva contable no sustituye esa obligación.

Indemnización: estimación proporcional de antigüedad (30 días/año en los primeros tres años, 20 después, máximo 150), distinguida del importe aprobado al liquidar. No se considera un pago incondicionalmente exigible para todos los ceses. La liquidación conserva la revisión explícita del motivo/aplicación de indemnización y ajusta la reserva al importe aprobado. No automatiza casos especiales de confianza/reintegro ni prestaciones convencionales adicionales.

## Liquidación final

El borrador calcula solo vacaciones pendientes; tomar o pagar vacaciones invalida un borrador anterior. Salario/saldo cambiado exige recalcular. Antes del pago deben cerrarse las planillas que incluyen el descanso; no se permite un descanso que exceda la terminación. La liquidación cancela los pasivos de vacaciones/aguinaldo/indemnización y los ajusta hasta la aprobación final; adiciones se llevan a gasto y descuentos a sus cuentas de origen. Las cuotas planificadas conservan detalle y se reconocen en el saldo de la deuda cuando se paga la liquidación. Los descuentos ordinarios no consumen el aguinaldo protegido. Los salarios ordinarios pendientes se pagan en su planilla, separadamente de las prestaciones finales.

Pago neto: diario de caja/banco obligatorio. Efectivo: recibo y egreso de caja una vez. Banco: sin caja. Cierra el perfil y fecha histórica pagada en la misma transacción. Historial ya pagado sin vínculo contable queda intacto; no se generan pagos retrospectivos.

## Marco y alcance

Código del Trabajo de Nicaragua, artículos 76–79 (vacaciones), 42–45 (terminación/antigüedad) y 93–98 (aguinaldo). Fuente oficial: https://www.poderjudicial.gob.ni/cjnejapa/pdf/codigo-trabajo.pdf . El valor implementado usa salario ordinario fijo del perfil; salario variable necesita promedio de seis meses para vacaciones y la base correspondiente para aguinaldo, que no se infiere aquí. El prorrateo por mes calendario es la política aritmética del módulo, no una cita literal de una fórmula diaria de la ley.

## Verificación

Pruebas de calendario normal/bisiesto, prorrateo, historial pagado, provisión idempotente, revaluación salarial, saldo negativo, pago bancario/reversa, superposición/cancelación de descanso y aplicación contable del descanso. Base SQLite aislada para pruebas permanentes y QA transaccional PostgreSQL con rollback para efectivo/ROC, cierre de planilla y liquidación bancaria. WebKit verifica escritorio/móvil, navegación y campos de descanso/pago. Ningún pago ni descuento real se ejecutó.
