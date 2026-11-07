# Corrección revisada de fecha de ingreso · Pacas Holl

En **Planilla → Perfiles laborales**, cada trabajador activo con perfil tiene el enlace **Corregir fecha y recalcular**. También se puede acceder desde su fila de **Vacaciones y prestaciones**. Solo un administrador con acceso a la sucursal puede operar.

1. Elegir la fecha correcta, hasta hoy en Nicaragua.
2. Comparar vacaciones disponibles, aguinaldo acumulado e indemnización estimada con las fórmulas actuales del sistema. Revisar los borradores afectados.
3. Escribir motivo/sustento, confirmar la revisión y guardar.

La operación sincroniza `contract_start` del perfil y `hire_date` de la ficha. Conserva los cortes de prestaciones pagadas, los movimientos de vacaciones y las liquidaciones pagadas. Recalcula únicamente al empleado en las planillas en borrador ya calculadas, sin asiento ni pago asociado, y actualiza las prestaciones/totales de sus liquidaciones en borrador conservando adiciones y deducciones. Las planillas cerradas, pagadas o contabilizadas no se reescriben.

Si está activa la provisión automática, se regulariza su diferencia mediante un asiento nuevo con fecha de hoy y referencia `FECHA-ING-{audit_id}`, manteniendo los asientos anteriores. Si está desactivada y no existen provisiones, solo se actualizan los derechos y borradores; no se activa la contabilidad. Una provisión existente desactivada requiere revisar su configuración antes de operar.

Se bloquean fechas futuras, saldos de vacaciones negativos hoy o en las fechas de movimientos históricos aún vigentes, períodos contables cerrados cuando se necesita ajuste, deducciones excesivas en liquidaciones borrador y netos negativos en planillas recalculadas. Un trabajador inactivo/liquidado requiere revisión de su liquidación; esta pantalla está destinada a trabajadores activos. No realiza pagos ni borra movimientos.

Todo se aplica en una sola transacción bajo el bloqueo contable/de planilla y de filas del empleado. La comparación está firmada y ligada al usuario, empleado, salario, fechas, cortes, provisiones, movimientos y borradores. Si cambian los datos revisados se exige una comparación nueva. Una operación repetida no duplica la corrección.

La tabla `payroll_start_date_changes` conserva fecha anterior de cada ficha, fecha nueva, motivo, actor, hora de Nicaragua, comparación antes/después, IDs de borradores recalculados y comprobante de ajuste. Se crea en el arranque normal o mediante `PYTHONPATH=. venv/bin/python scripts/create_payroll_start_date_audit.py` desde backend. El script solo crea la tabla.

Pruebas: `python -m unittest tests.test_payroll_start_date tests.test_payroll_benefits tests.test_accounting_payroll`; navegador en empleados ficticios: `python tests/browser_payroll_start_date.py`.

Concurrencia en PostgreSQL local aislado: `python tests/postgres_payroll_start_date.py`. Dos confirmaciones simultáneas generan una sola corrección y un único ajuste.
