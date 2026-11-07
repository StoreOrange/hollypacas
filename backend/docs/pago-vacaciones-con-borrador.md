# Pago directo de vacaciones con liquidación pendiente

El pago directo (efectivo, transferencia o cheque) puede coexistir con una liquidación **en borrador** si se confirma expresamente que se descontará de ella. El formulario muestra la liquidación pendiente al elegir al trabajador, exige marcar la confirmación y muestra las bodegas por su nombre. Si hay un único diario válido y una bodega principal de la sucursal, los selecciona por defecto.

La transacción valida que el cese del borrador no sea anterior al pago, que su salario/saldo de vacaciones esté actualizado, que haya días disponibles y que las deducciones del borrador sigan siendo admisibles. Registra el pago y, si es efectivo, el egreso/recibo de caja; reduce vacaciones, bruto y neto del borrador. No cambia aguinaldo, indemnización, adiciones ni deducciones. La liquidación final paga solo el remanente. La nueva tabla `payroll_vacation_draft_adjustments` enlaza el pago con el borrador y conserva importes antes/después, actor y hora de Nicaragua.

Un fallo revierte todo. La operación conserva su clave idempotente, por lo que repetir el mismo envío no duplica el recibo ni el descuento del borrador. Una caja cerrada sigue bloqueada. Si la fecha de cese del borrador ya pasó o el borrador está desactualizado, se pide anularlo y calcular uno correcto; no se cambia silenciosamente su fecha. El descanso remunerado mantiene la protección anterior frente a liquidaciones pendientes. Las liquidaciones pagadas no se editan.

Despliegue: `PYTHONPATH=. venv/bin/python scripts/create_vacation_draft_adjustments.py` crea solo la tabla de auditoría; no paga vacaciones ni cambia liquidaciones.

Pruebas: `python -m unittest tests.test_vacation_cash_draft tests.test_payroll_benefits tests.test_payroll_start_date tests.test_accounting_payroll`. UI aislada, sin enviar pago: `python tests/browser_vacation_cash.py`.

El pago del saldo completo acepta la precisión visible de cuatro decimales en días (tolerancia máxima 0.00005 día) y utiliza el valor monetario real del saldo, evitando rechazos por redondeo, sobrepagos y residuos menores a esa precisión.
