Publicación de la interfaz móvil autorizada el 27 de octubre de 2026. Se conservan las validaciones de cantidades, promociones, existencias y envío.

# Preventa móvil

Solo Holl Pacas. Se mantiene el flujo existente de registro y facturación, con CSS/JS de presentación separados. Publicación en Git y VPS autorizada junto con la interfaz móvil.

La cabecera oscura se sustituye por una presentación azul pastel con sucursal/bodega, iconos Bootstrap y accesos secundarios en un desplegable. La navegación Cliente → Productos → Revisar desplaza a cada sección y enfoca sus controles. Historial y actividad del vendedor quedan debajo del formulario. Se conservan IDs, nombres de campos, controles de cliente, vendedor, búsqueda, combo, promociones, carrito, peso, congelado y envío. Las otras empresas mantienen su disposición anterior.

Interacciones: foco suave, selección de pasos, entrada discreta de productos, destello breve del total al agregar una fila, respuesta al toque y estado de envío con indicador indeterminado. La confirmación muestra éxito solo con la preventa que devolvió el servidor; no simula un porcentaje de guardado. Una preventa congelada se identifica como tal. Se respeta movimiento reducido y no se intercepta el desplazamiento manual.

El total estimado, bultos y acciones de congelar/enviar permanecen visibles. Precios, cantidades, promociones y validación de stock conservan sus handlers y reglas. El paquete de dos unidades sigue bloqueado y se envía por el formulario original.

Se retiraron del renderizado Holl los mensajes ofensivos y emojis del indicador de rendimiento. Se conserva el dato de bultos facturados, vendedor e historial con texto neutral; no se cambian conteos ni criterios del backend.

QA local: búsqueda de cliente y producto, edición y total, bloqueo de pedido vacío, combo, congelado, confirmación normal/congelada, selección y bloqueo del paquete de dos unidades. Pruebas backend de promociones, inventario, productos por libra y pendientes. Navegador a 320/375/390/430/768 px y movimiento reducido. Comprobación de la aplicación local real con solo GET/HEAD: no se envía ninguna preventa de prueba a la base de uso.

Refinamiento de iconos y organización: cliente/vendedor separados visualmente; búsqueda y resultados antes de promociones; iconos Bootstrap de personas/productos en resultados y carrito. Cantidad, precio, existencia, quitar producto y resumen llevan iconos contextuales. La decoración del DOM conserva inputs, eventos y datos del formulario; las cantidades protegidas siguen deshabilitadas.
