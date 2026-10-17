# Motorizado · Pacas Holl

Módulo exclusivo de Pacas Holl, URL `/motorizado`, con menú y diseño responsive. No requiere instalar una app ni configurar un agente. El icono representa una motocicleta de reparto con caja trasera (SVG propio, también en el menú y acceso móvil).

## Usuarios

En Datos → Usuarios asignar `motorizado`, sucursal y bodega. El usuario autenticado es el agente; solo opera sus propios viajes. El rol tiene prioridad sobre roles adicionales, ve únicamente Motorizado y entra directamente a él. Inicio regresa a su módulo. Un control global consulta el rol actual en un hilo de trabajo antes de ejecutar otras rutas: redirige navegación externa y rechaza otras acciones/APIs con 403, mediante cookie o Bearer. Conserva login, logout y recursos estáticos. Otros entornos no reciben esta restricción.

El vendedor debe tener `vendedor_id` vinculado en Usuarios, sin inferir identidad por nombre. Conserva su vista de preventas móviles y dispone de **Mis envíos y costos de transporte**; accede a sus costos, confirma recepciones y anula sus propios viajes pendientes. Puede regresar a sus preventas. Administración ve las sucursales autorizadas, pero no puede confirmar o anular en nombre del vendedor sin ese rol/vínculo; un usuario con rol motorizado sigue siendo exclusivamente motorizado.

## Registro y contabilidad, separados de las cajas de ventas

El motorizado selecciona vendedor activo, destino, monto C$ y comentario opcional. Fecha y agente se toman del servidor/sesión. Otro destino permite escribir dirección. El monto admite formato `1,250.50`, máximo dos decimales y validación Decimal.

Guardar registra PENDIENTE y un comprobante contable POSTEADO: **Debe 6104 Transporte y Fletes (61 Gastos Operativos) / Haber Banco**. Usa un diario BANK activo en C$ y una cuenta dentro de 1102 Bancos. La configuración estándar BANCO sin cuenta se completa con 1102 si está activa y corresponde a Bancos. Si hay varios diarios se elige el banco; no se admite una cuenta de caja de ventas.

**Los viajes nuevos no crean ReciboCaja ni modifican CajaDiaria ni CierreCaja. Tampoco consultan el cierre de ventas para permitir su registro o anulación. No alteran arqueos actuales o históricos.** Los cierres contables mensuales/anuales sí se respetan, porque existe un asiento real. Registrar el comprobante no ejecuta ninguna transferencia bancaria. El gasto y sus datos se guardan en una sola transacción, con clave de operación para impedir duplicados.

El motorizado marca ENTREGADO (indicador verde). El vendedor asignado confirma RECIBIDO. PENDIENTE tiene indicador rojo. Solo el vendedor asignado anula un viaje PENDIENTE, con motivo y confirmación de devolución/cancelación del pago. Se genera reversión contable con las cuentas originales, sin recibos ni movimientos en cajas de ventas. Un gasto conciliado requiere deshacer la conciliación antes de revertirlo. Los estados de entrega y recepción no generan otro asiento.

## Fotos de soporte

Se adjuntan opcionalmente con el nuevo viaje o posteriormente en **Fotos de soporte**, incluso después de entregar o anular. El motorizado propietario, el vendedor asignado y administradores autorizados pueden añadir/ver los soportes. Cada carga registra actor, fecha y evento. Se conservan como evidencia; no hay borrado desde la interfaz.

Hasta 6 fotos por viaje, JPG/PNG/WEBP, 8 MB cada original y hasta 24 megapíxeles. Se verifica contenido, se orienta, reduce a 1800 px y normaliza a JPEG sin metadatos EXIF. Las imágenes se almacenan transaccionalmente en una tabla dedicada, incluidas en el respaldo de base de datos. Sus bytes se cargan solo al consultar una foto; los listados solicitan únicamente metadatos. La ruta privada valida acceso al viaje antes de entregar contenido, con no-store y nosniff. Una imagen inválida al registrar impide guardar el viaje y su gasto; una carga posterior no crea otro gasto. Las fotos repetidas se deduplican por contenido.

La interfaz ofrece vista previa y conserva el panel abierto durante la selección, sin que la actualización automática lo sustituya. Tras subir fotos a un viaje histórico se vuelve a la fecha del viaje.

## Paneles e historial

Resumen por fecha/filtros: viajes registrados, total cobrado neto o costo de transporte, entregados/confirmados y pendientes. Anulados siempre visibles e importes anulados separados del costo neto. El panel agrupa por vendedor y los totales abarcan todo el filtro, no solo la página de 30 viajes. Cada tarjeta conserva proceso, actores, fechas, comentarios, soportes y comprobantes contables. Actualiza cada 20 s al estar visible y no reemplaza paneles abiertos. Los indicadores son estados visuales, respetando reducir animaciones.

Destinos: terminales Mayoreo/Rigoberto Cabezas, Roberto Huembes, Israel Lewites y sector UCA; Tipitapa, Masaya, Ciudad Sandino, Mateare, Ticuantepe, Nindirí, El Crucero y Granada; Otro destino libre. Referencia de terminales: [Guía turística de Nicaragua, Transporte interno](https://www.el19digital.com/files/notas/source/2023/Mayo/22May/NOTAS/Guia-Turistica.pdf). No incluye horarios ni tarifas de buses.

## Primera versión local

Se agregan columnas de comprobante/reversión y tabla de fotos, manteniendo referencias a recibos antiguos como evidencia. La rutina explícita `core/motorizado_legacy.py:isolate_legacy` no se ejecuta automáticamente: permite revertir el asiento anterior, registrar el gasto contra Banco, excluir recibos antiguos del arqueo y restaurar su efecto previo en CajaDiaria, conservando auditoría y sin editar cierres guardados. Es idempotente. Se aplicó al único viaje local anterior de C$100.

## Verificación

```sh
.venv/bin/python -m unittest tests.test_motorizado tests.test_motorizado_access tests.test_motorizado_photos tests.test_pos_browser tests.test_pos_automation tests.test_accounting_cash_receipts
.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8018 --timeout-graceful-shutdown 3
# Con servidor local en marcha:
.venv/bin/python tests/browser_motorizado.py
```

Servicios financieros en SQLite aislada: gasto bancario, reversión e idempotencia, banco sin cuenta de ventas, cierres de ventas intactos, cierre contable, permisos, aislamiento, reclasificación anterior, fotos privadas/duplicadas/límites y rollback. Firefox valida registro móvil, multipart, vista previa, soportes posteriores, estados, historial, menú e icono sin desbordamiento. Solicitudes de pagos simuladas: no genera transferencias ni egresos reales en esas pruebas.

## Historial visible y selector de vendedor

La pantalla muestra únicamente el historial operativo: registro del viaje, entrega, recepción, anulación y fotos, con actor/fecha y comentarios del usuario. No presenta reclasificaciones, números de recibos ni comprobantes internos en ese historial. El botón Historial de viajes y vendedores abre todo el período disponible para ese usuario; fechas y vendedor permiten acotarlo respetando su acceso. Cada selección de vendedor utiliza un único combobox con búsqueda integrada (Select2 existente), sin un campo de búsqueda adicional. El filtro del motorizado incluye los vendedores de sus viajes históricos, también inactivos, y el registro nuevo conserva el catálogo de vendedores activos.

## Pagar y cerrar · archivo y reporte diario

Solo el vendedor vinculado al viaje puede confirmar su pago y cierre. Requiere estado RECIBIDO (recepción ya confirmada), confirmación explícita de pago y una clave de operación. Se puede cerrar un viaje o un grupo del mismo motorizado/sucursal; cada operación admite hasta 100. El cierre conserva motorizado, vendedor, usuario que confirmó, fecha/hora, total, cantidad y comentario. Bloquea los viajes en orden estable; la repetición de la misma solicitud devuelve el mismo cierre y un viaje cerrado no puede pagarse dos veces. Los cambios de estado de un viaje cerrado están bloqueados; sus fotos e historial siguen accesibles.

La contabilización existente al registrar el viaje no se repite: cerrar es confirmar el pago efectuado por el vendedor, no generar otro gasto ni ejecutar una transferencia. No modifica recibos, CajaDiaria, cierres de ventas ni períodos contables.

La vista principal contiene únicamente viajes sin cierre y no anulados, incluyendo pendientes de días anteriores. Pagados/cerrados y anulados quedan en Historial de viajes y vendedores. El motorizado los deja de ver en pendientes durante la actualización automática (20 s, sin reemplazar paneles que tenga abiertos). La sección de viajes confirmados permite al vendedor cerrar grupos o cada viaje individual.

Reporte de cierres: `/motorizado?view=cierres`. Por defecto consulta los cierres de hoy; el filtro corresponde a la fecha del pago/cierre, aunque el viaje fuera de días anteriores. Muestra cierre, motorizado, vendedor, quién confirmó, total y desglose de viajes con enlaces a su historial. El motorizado solo consulta sus propios cierres, el vendedor los de su identidad y administración sus sucursales autorizadas. Los totales cubren todo el período filtrado, con paginación de 30 cierres.

Regresión adicional: `python -m unittest tests.test_motorizado_closures`; Firefox local con API/SQLite aislada: `python tests/browser_motorizado_closures.py`. Verifica pago/cierre, limpieza de pendientes, permanencia en historial, reporte por fechas, permiso del vendedor, duplicados y ausencia de segundo gasto o efecto en cajas de ventas.
