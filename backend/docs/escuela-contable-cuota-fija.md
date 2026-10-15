# Registro guiado para Pacas Hollywood

Versión local revisada el 13/10/2026. Régimen **declarado por el usuario**: cuota fija. Esta funcionalidad asiste la contabilidad interna; no determina elegibilidad tributaria, liquidaciones, retenciones ni presenta declaraciones oficiales.

## Funcionamiento

El usuario describe la operación o elige uno de 13 casos. La orientación de texto es una regla determinista: si detecta categorías incompatibles no elige ninguna. El sistema propone cuentas activas del catálogo por nombre y tipo contable, deja las cuentas visibles para revisión y pide monto y referencia. No crea cuentas ocultas ni publica asientos. Para depreciación, la cuenta de gasto debe existir y el importe debe proceder de un cálculo revisado.

La propuesta utiliza centavos enteros, admite pago parcial en adquisiciones y separa venta de costo de ventas. Se revisa antes de cargar; las líneas quedan editables y la publicación sigue el flujo manual vigente. No sustituye líneas que ya fueron digitadas. La selección «ya registrada en el sistema» impide cargar la propuesta y remite a revisar el asiento automático; no es una detección exhaustiva de duplicados entre documentos.

Los asientos contables no modifican unidades físicas, saldos operativos de clientes/proveedores ni caja operativa. Los módulos de inventario, venta, pago o cobro ejecutan esos movimientos. El usuario debe conciliar el asiento con su documento de origen y verificar si el módulo ya lo contabilizó.

## Enseñanza y alcance fiscal

- Partida doble, entidad económica, devengo, soporte documental y costo histórico.
- Mercancía comprada: inventario; mercancía vendida: costo de ventas. Los impuestos no recuperables y costos atribuibles se incluyen en el costo cuando corresponde; no se inventa crédito fiscal.
- Servicios consumidos: gasto del período; anticipos: derechos/activos según la operación.
- Pago de deuda y cobro de cliente: reducción de pasivo o activo; no duplican compra/gasto/venta.
- Aporte y préstamo: patrimonio y pasivo; no ventas.
- Cuota fija: importe notificado/pagado con BIT o recibo y período; no calcula tarifas.
- Vida útil, valor residual, retenciones, importaciones, nómina y ajustes complejos requieren cálculo y clasificación específicos; el registro manual permite sus líneas adicionales.

La guía no asegura cumplimiento integral de PCGA, NIIF o legislación fiscal por el solo uso de la interfaz. La elección de marco contable y tratamiento tributario específico se revisa con el contador y la inscripción fiscal vigente.

## Fuentes primarias consultadas

- DGI, obligaciones de pequeños contribuyentes, con referencia al artículo 258 LCT: https://www.dgi.gob.ni/FAQ/requisitos_de_inscripcion.htm
- DGI, obligaciones de declaración según régimen: https://www.dgi.gob.ni/FAQ/declaracion_mensual_de_recaudo.htm
- IFRS Foundation, NIC 2, reconocimiento del costo de inventarios y costo de venta: https://www.ifrs.org/issued-standards/list-of-standards/ias-2-inventories/

## Verificación

Reglas de 13 operaciones, balance en centavos, pagos parciales, venta con costo, rechazo de importes inválidos y orientación ambigua. Pruebas WebKit de carga de propuesta, protección de líneas existentes, origen automático, diseño móvil y navegación Enter. No se publicaron comprobantes durante QA.
