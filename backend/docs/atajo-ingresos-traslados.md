# Ingreso rápido de artículos · Miss Zapatos

Botón verde visible junto a «Buscar variantes» en Ventas, en Traslados rápidos y al pie de ambos buscadores. Abre el formulario existente de ingresos por unidad dentro de un modal, sin abandonar ni enviar la venta o el traslado.

Se heredan la bodega vigente de origen (no el destino) y el texto buscado. El formulario permite elegir modelo o variante con color y talla, cantidad inicial 1, revisar la lista y registrar con la confirmación habitual. No se guarda nada al abrir. El modo integrado oculta el menú lateral; «Volver y continuar» restaura el buscador anterior y actualiza sus resultados después de un ingreso. Una lista pendiente requiere confirmar antes de descartarse. Ctrl/Cmd+clic conserva el acceso en otra pestaña.

Se mantienen el permiso `access.inventory.ingresos`, el alcance por sucursal, las validaciones del servidor y la protección contra duplicados. El acceso no concede permisos nuevos. Administradores de Miss Zapatos pueden seleccionar las tiendas activas autorizadas de la empresa.

Verificación: `python -m unittest tests.test_shoe_receipt_shortcut tests.test_shoe_unit_receipts tests.test_shoe_cashier_transfers tests.test_quick_transfer_exact_search` y `python tests/browser_shoe_receipt_shortcut.py`. Las pruebas de navegador verifican origen, búsqueda heredada, modal en escritorio/móvil y regreso al buscador; bloquean POST para no crear inventario.
