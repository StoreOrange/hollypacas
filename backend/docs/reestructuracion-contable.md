# Reestructuración contable de Pacas Hollywood

Objetivo: adoptar un proceso de registro inspirado en Odoo Accounting dentro del ERP existente. El alcance corresponde a contabilidad; facturación comercial, inventario y laboratorios conservan sus interfaces e integraciones.

## Base implementada localmente

- Panel con pendientes, movimientos publicados del mes y acceso a comprobantes e informes.
- Estados: BORRADOR (editable, fuera de libros), POSTEADO (publicado) y REVERSADO (original publicado con contrapartida). ANULADO se conserva para el historial y cancelación de borradores.
- Publicación verifica cuentas activas, montos válidos, líneas y cuadre; registra usuario y fecha de publicación.
- Corrección mediante asiento inverso vinculado al original, con motivo, fecha y usuario. No admite reversión duplicada ni una fecha anterior al original. Ambos asientos permanecen en los libros.
- Diario, mayor, balances e informes exportables excluyen borradores. Las anulaciones históricas no se reinterpretan automáticamente.
- Numeración existente conservada; nuevas secuencias consideran todas las sucursales y usan bloqueo transaccional en PostgreSQL.
- Cierres mensuales y anuales, administrados por el rol administrador. Solo se cierran períodos terminados, con motivo y sin borradores pendientes. Se permite reabrir y volver a cerrar, conservando fecha, actor y motivo en cada evento. Los registros manuales, importación de apertura y generadores automáticos verifican todos los cierres vigentes con el mismo bloqueo transaccional. Los cierres anteriores por fecha se adaptan sin reactivarse después de una reapertura.
- Un período cerrado se corrige mediante reversión en un período posterior abierto o reapertura auditada de las fechas afectadas. Reabrir un mes no levanta un cierre anual vigente, ni reabrir un año levanta automáticamente los cierres mensuales. La pantalla explica los bloqueos superpuestos. Los cierres protegen las fechas; los ajustes y regularizaciones del ejercicio se registran como comprobantes antes del cierre.
- Los datos históricos no se borran ni se regeneran. Los nuevos campos de publicación de registros antiguos quedan vacíos porque no se puede deducir su autor o fecha de publicación.

- Diarios especializados: ventas, compras, caja, banco y operaciones diversas; configuración de cuenta de liquidez, sucursal y moneda. Los números de comprobante existentes se conservan.
- Adaptación del historial: referencias exactas AUTO-VTA se agrupan en Ventas; las demás operaciones en Operaciones diversas. Se recupera el cliente solamente cuando existe la factura de origen verificable. No se infieren contactos por texto ni se recalculan importes.
- Conciliación parcial y total por cuenta, sucursal y contacto. Saldos residuales, límite de aplicación y posibilidad de deshacer conservando auditoría.
- Registro manual de movimientos de extracto, conversión C$/USD y conciliación contra líneas del libro sin duplicar asientos. Rechaza sobreaplicación y repetición exacta de un movimiento.
- Contrapartida bancaria para un movimiento sin asiento previo, publicada y conciliada en una misma transacción. No vuelve a generarse al deshacer un vínculo existente.
- Un asiento conciliado debe desconciliarse antes de revertirlo.
- Pantalla de registro enteramente manual: búsqueda por código/nombre, líneas editables, Debe/Haber, cuadre visible, borradores incompletos y publicación estricta. Se elimina el asistente inteligente de la interfaz y la inferencia de cuentas para registros manuales de Hollywood.
- Identificador de operación único para evitar duplicación por reenvío del formulario.
- Navegación de las líneas con Enter: seleccionar cuenta con flechas/Enter, detalle, Debe o Haber, y siguiente línea. Enter no publica accidentalmente el comprobante.
- Selector visible de todos los tipos activos (Diario, Ingreso, Egreso, Ajuste y tipos personalizados), independiente del tipo predeterminado del diario.
- Catálogo contable con jerarquía, búsqueda, cuentas y subcuentas, nombres y descripciones. Códigos sugeridos por ubicación, máximo de cuatro niveles, detección de ciclos y recalculo de niveles al mover una cuenta.
- Las cuentas con movimientos conservan código y tipo; se permite mejorar nombre y descripción. Las configuraciones del catálogo y tipos no se sobrescriben al reiniciar.
- Creación y edición del catálogo en otra pestaña; actualización del formulario conserva las líneas y montos en curso.



## Continuación de la reestructuración

- Auditar todas las integraciones históricas de cobros, compras, devoluciones, anulaciones comerciales y producción contra sus asientos. El historial operativo no se reconstruye ni se regenera desde suposiciones.
- Completar clasificación financiera y configuración de asociaciones automáticas; eliminar progresivamente la inferencia por nombres en generadores automáticos.
- Ampliar documentos de proveedor y pagos guiados, importación masiva de extractos, antigüedad de saldos y tratamiento de diferencias cambiarias. La conciliación actual trabaja en C$ del libro mayor.
- Ensayar la migración con una copia de la base de producción y comparar saldos por cuenta/documento. Las pruebas de concurrencia en PostgreSQL y la auditoría completa de automatizaciones están pendientes.

## Decisiones confirmadas por el usuario

- Mantener y adaptar el historial actual.
- Contabilidad interna completa; los reportes fiscales oficiales quedan fuera del alcance.
- Registro manual ágil; retirar la partida inteligente de la interfaz de comprobantes.

## Referencias del proceso

- [Diarios de Odoo 19](https://www.odoo.com/documentation/19.0/applications/finance/accounting/get_started/journals.html).
- [Conciliación bancaria de Odoo 19](https://www.odoo.com/documentation/19.0/applications/finance/accounting/bank/reconciliation.html).
- [Bancos y caja en Odoo 19](https://www.odoo.com/documentation/19.0/applications/finance/accounting/bank.html).

## Verificación inicial

Pruebas aisladas con SQLite para publicación, cuadre, borradores incompletos, cuentas inactivas, sucursales, numeración, reversión, cierre, bloqueo de períodos, adaptación del historial, aplicaciones parciales, conciliación bancaria e idempotencia. Prueba visual en navegador local de escritorio y móvil, sin crear asientos reales. Las pruebas de PostgreSQL con operaciones concurrentes y la comparación del historial real forman parte del ensayo de migración pendiente.
