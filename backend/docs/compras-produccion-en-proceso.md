# Compras para producción en proceso · Holl Pacas

Esta fase registra la inversión en compras en los libros oficiales usando **1112 · Inventario de producción en proceso**, activo corriente de naturaleza deudora, hermano de 1104 Inventarios bajo 11 Activo Corriente. La cuenta inicia sin saldo. No se crea una bodega ni se reclasifican comprobantes anteriores.

En Contabilidad → Asientos → Registro guiado, seleccionar **Compra · inventario de producción en proceso** (opción inicial en Holl Pacas). Indicar importe, forma de pago, soporte y si la operación aún no está contabilizada. Revisar propuesta, cargar, completar fecha/proveedor/moneda y publicar. La propuesta usa 1112 al Debe y caja, banco o proveedores al Haber, incluyendo pago parcial. Selecciona el diario Compras y conserva la moneda de captura. Los libros convierten USD a C$ con la tasa del comprobante mediante el mecanismo existente.

La guía exige la cuenta exacta 1112, activa, BALANCE, DEBE y con nombre esperado. Si falta, bloquea la propuesta. Las líneas cargadas siguen siendo revisables/editables por el contador. La opción de inventario general sigue disponible para otros registros explícitos; no usarla para estas compras.

No aumenta unidades, no actualiza costos de productos ni bodegas, no publica costo de ventas y no aplica automáticamente el saldo a producción. El saldo de 1112 aparece en el balance y en los reportes contables de esa cuenta, separado de 1104. Su posterior aplicación requiere otra fase; no representa por sí solo una conciliación con las existencias físicas.

La política global de contabilización automática se mantiene desactivada. No se modifica ninguna factura ni importe histórico. Buscar soportes en el buscador de comprobantes para evitar duplicarlos. Se mantienen las validaciones de permisos, sucursal, período, cuadre y reintento de envío.

Desde backend, en la empresa activa hollywood_pacas, ejecutar primero `venv/bin/python scripts/configure_production_inventory_account.py` y luego agregar `--apply --audit-file /ruta/exclusiva/configuracion-1112.json`.

El script bloquea ejecuciones concurrentes, comprueba empresa/cuenta padre, rechaza conflictos de código/nombre y es idempotente. Crea exclusivamente la cuenta, con archivo previo de auditoría, sin asientos ni movimientos.

Validación: propuestas contado/banco/crédito/parcial; publicación SQLite limitada a tablas contables; navegador con pago parcial en USD conservando moneda y cuenta separada. Ninguna factura de prueba se publica en producción.
