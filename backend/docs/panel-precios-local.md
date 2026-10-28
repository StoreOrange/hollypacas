# Cambiar precios · Miss Zapatos (local)

Ruta: `http://127.0.0.1:8001/inventory/shoes/prices`.

Panel integrado con menú, búsqueda por código/nombre/color/talla y siete listas en C$. Acceso «Cambiar precios» inmediatamente debajo de Ventas, incluso con un orden de menú guardado. Usa el permiso existente `access.inventory.productos` y solo se ofrece en modo zapatos.

La selección es por producto completo; incluye todas sus tallas y colores. Los resultados agrupan variantes por producto y permiten desplegar su detalle. La existencia mostrada suma las variantes encontradas. Se muestra un máximo de 120 variantes por búsqueda, indicado al alcanzar el límite.

Se requieren los siete precios antes de revisar. La generación de escala es opcional. Antes de guardar se revisan productos y precios; se avisa si alguna lista tiene precio cero y se evita enviar dos veces mientras se guarda. La búsqueda y el guardado reutilizan los handlers originales, sin cambiar reglas, costos ni existencias. El acceso anterior desde Ingresos abre este panel.

Validación: `tests/test_shoe_prices_panel.py` y `tests/browser_shoe_prices_panel.py`. El navegador usa la aplicación local real, búsquedas sintéticas y guardado interceptado; no modifica precios reales. También se comprobó la búsqueda real mediante GET.

Desarrollado y probado localmente el 28/10/2026. Publicación en Git autorizada posteriormente por el usuario. Sin despliegue en la VPS. Respaldo previo en `../backups/panel-precios-local-20261028-110058/`.

El buscador ofrece sugerencias manuales dentro del panel tras una pausa breve al escribir. Cada variante muestra código, color, talla y existencia separada en Central, KG y KGF. Se elige con clic o flechas/Enter; Escape cierra la lista. Escanear y pulsar Enter mantiene la búsqueda directa. Elegir una opción no selecciona productos para guardar ni agrega filtros de color/talla automáticamente. Las consultas atrasadas se cancelan y no reemplazan opciones actuales. Validado en `tests/browser_shoe_price_suggestions.py`, con pruebas de existencias contrastadas contra la base local y sin escrituras reales.

Al guardar se presenta una confirmación grande «¡Precios actualizados!» con el código y las tallas/colores de cada producto realmente actualizado. El servidor entrega ese detalle después de confirmar la transacción, incluyendo variantes fuera del filtro de búsqueda. El mensaje permanece hasta cerrarlo o pulsar «Seguir trabajando», que devuelve el foco al buscador. Un error de guardado conserva la revisión y no muestra éxito. Verificado con SQLite aislada (incluido fallo de commit) y navegador con respuestas simuladas; no se modifican precios reales para comprobarlo.

Acceso exclusivo al rol `administrador`: se oculta el enlace del menú y de Ingresos para otros roles. El servidor responde 403 tanto en la página como en el endpoint de guardado, aunque el usuario tenga `access.inventory.productos`. No se cambia el acceso al buscador compartido de Ventas.
