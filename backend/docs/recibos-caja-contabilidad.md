# Recibos de caja y contabilidad · Pacas Hollywood

Integración local para nuevos recibos que afectan caja. Los recibos anteriores conservan su información y muestran «Sin asiento vinculado»; no se generaron asientos retrospectivos ni se movieron saldos históricos.

Un registro confirma conjuntamente el recibo, la actualización de CajaDiaria y el asiento POSTEADO. Ingreso: Debe Caja / Haber contrapartida. Egreso: Debe contrapartida / Haber Caja. La cuenta del rubro tiene prioridad; si no existe, se pide una cuenta explícita en el formulario. Solo se sugiere una coincidencia exacta única de nombre, nunca se supone que todo pago es gasto. Una deuda puede cargarse a su pasivo, un anticipo a su activo, etc.

El diario CASH debe estar activo y pertenecer a la sucursal o ser general. El diario estándar CAJA sin cuenta se completa únicamente con la Caja 1101 activa y de naturaleza Debe del catálogo existente. No modifica una configuración elegida previamente. La cuenta de contrapartida debe estar activa y ser distinta de Caja.

La operación usa Decimal y redondea equivalentes a dos decimales; USD requiere tasa válida. El asiento funcional está expresado en C$. Una clave única del formulario impide duplicar recibo, saldo y asiento al reintentar. El movimiento continúa entrando al cierre por ReciboCaja; no se suma otra vez por el asiento.

Se rechazan caja cerrada, período contable cerrado, cuenta ausente/inactiva y datos inválidos. El cierre de caja comparte el bloqueo transaccional contable con el recibo. Revertir aisladamente un asiento vinculado se bloquea: la corrección debe coordinar recibo, caja y asiento. No se añadió una anulación de recibos en esta etapa.

Si se desmarca «Afecta caja», no se crea movimiento de CajaDiaria ni asiento automático de caja. Para movimientos bancarios o gastos fuera de caja se utiliza su documento y flujo contable correspondiente.

El asiento vinculado aparece junto al recibo en el historial. La vista previa utiliza las cuentas reales del diario y de la contrapartida, y muestra importes con separador de miles.

Verificado: monto y tasa, conversiones, asiento balanceado, egreso inmediato, selección de contrapartida para rubro sin configuración, idempotencia, rollback completo y UI WebKit. Todas las escrituras QA se revirtieron; no se imprimieron recibos físicos.
