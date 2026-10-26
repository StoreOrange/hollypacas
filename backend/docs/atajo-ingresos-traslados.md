# Solicitar ingresos a la bodega origen · Miss Zapatos

Botón verde en Traslados rápidos y al pie de sus búsquedas de variantes. También se incluye en el buscador de variantes de Ventas. Abre `/inventory/ingresos-unidad?bodega_id=...` en una pestaña nueva y preselecciona la bodega que origina el traslado (o la bodega de venta en el buscador de Ventas).

El enlace consulta la selección vigente al pulsarlo; no toma la bodega destino ni fuerza la bodega predeterminada. No registra ni autoriza ingresos por sí mismo: continúa en el formulario existente por modelo, color y talla, con sus validaciones y confirmación normal. El traslado original permanece abierto.

La página de ingresos valida la bodega indicada en el servidor. Los administradores de Miss Zapatos utilizan el mismo alcance de tiendas activas que las utilidades de inventario; los demás usuarios conservan sus sucursales autorizadas. Se mantiene el permiso `access.inventory.ingresos`; un usuario con permiso solo de traslados no obtiene permiso de ingresos por este enlace. El POST conserva la validación de bodega autorizada.

Verificación: `python -m unittest tests.test_shoe_receipt_shortcut tests.test_shoe_unit_receipts tests.test_shoe_cashier_transfers tests.test_quick_transfer_exact_search` y `python tests/browser_shoe_receipt_shortcut.py`. El navegador valida cambios de origen, navegación en nueva pestaña y selección Central/KGF en el formulario real, sin enviar POST de inventario. Las pruebas de catálogo aíslan sus permisos del usuario de prueba y las de notificaciones usan SQLite compatible con el pool de hilos.
