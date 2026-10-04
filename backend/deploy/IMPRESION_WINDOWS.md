# Etiquetas de Miss Zapatos en Windows

1. Instalar la impresora y su controlador en Windows; comprobar que imprime desde Windows.
2. Para impresión mediante vista previa, abrir el PDF y seleccionar la impresora en el visor. No requiere QZ Tray.
3. Para seleccionar la impresora dentro de Miss Zapatos y enviar sin vista previa, instalar QZ Tray desde https://qz.io/download/ en cada PC con impresora y mantenerlo abierto.
4. En Etiquetas, elegir “Esta PC — Windows / Mac (QZ Tray)” y pulsar “Conectar / actualizar”. Permitir la conexión de QZ Tray, seleccionar impresora y preparar cantidades.
5. Elegir envío directo y probar inicialmente una etiqueta de 50 × 20 mm (5 × 2 cm). Configurar ese tamaño también en las preferencias del controlador.

El PDF se genera en el servidor; QZ Tray lo recibe desde el navegador e imprime en la PC del cliente. No se envía a la impresora del servidor ni se cambia a otro destino al fallar. Las cantidades ya están incluidas como páginas: se envía una copia del documento.

Sin certificado y firma de confianza, QZ Tray puede mostrar autorizaciones. La integración actual omite la vista previa, pero no elimina estos avisos. Para operación totalmente silenciosa en producción se debe configurar firma de mensajes según https://qz.io/docs/signing ; las claves privadas deben permanecer en el servidor.

“Equipo servidor local” conserva únicamente las pruebas Mac/Linux en la misma computadora del servidor. No seleccionarlo para una PC Windows remota.

Validación pendiente de despliegue: conexión y autorización en Windows, enumeración del controlador real, tamaño físico y lectura del código de barras. No se ha probado físicamente en la PC del cliente.

Las etiquetas de zapatos utilizan el código corto de variante V000003 (ejemplo). El código largo del producto permanece intacto y sigue siendo consultable.

## Calibración por entorno

El tamaño inicial es 50 × 20 mm. La calibración guardada desde Datos se conserva en `backend/settings/miss_zapatos_labels.json`, excluido de Git para que una actualización no sobrescriba los ajustes de la impresora.

`backend/settings/miss_zapatos_labels.example.json` incluye una copia de la última calibración local (50 × 25 mm). Para trasladar exactamente esa configuración a una instalación nueva, copiarla a `miss_zapatos_labels.json` únicamente si no existe una calibración propia. También se puede ajustar desde Datos → Etiquetas de zapatos.

Los cambios se distribuyen desde la misma rama `main`. El inicio, las etiquetas y los flujos de zapatos se habilitan según el entorno activo; mantener la configuración local de empresas y su base de datos al actualizar.
