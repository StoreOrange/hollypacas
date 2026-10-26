(function(root){
'use strict';
const lessons={
 production_inventory:{title:'Compra para producción en proceso',debit:'Inventario de producción en proceso',credit:'Proveedor o medio de pago',explain:'Registra la inversión de compra como activo contable separado. No agrega unidades, no cambia costos de Central o Estelí y no registra costo de ventas.',support:'Factura del proveedor, referencia y evidencia del pago o deuda.',rule:'Control monetario de compras. La aplicación a producción y costo de ventas queda para una fase posterior.'},
 inventory:{title:'Compra de inventario',debit:'Inventarios',credit:'Proveedor o medio de pago',explain:'La mercancía para revender es un activo. Debe aumenta inventario; Haber registra el pago o la deuda. La compra no es todavía costo de ventas.',support:'Factura del proveedor y documento del ingreso de mercancía.',rule:'Costo histórico y reconocimiento del costo al vender (NIC 2).'},
 expense:{title:'Gasto del negocio',debit:'Gasto correspondiente',credit:'Proveedor o medio de pago',explain:'Reconoce el servicio consumido en el período. Debe aumenta el gasto; Haber disminuye dinero o reconoce una deuda. Un pago anticipado para meses futuros puede ser un activo, no gasto inmediato.',support:'Factura, recibo o soporte del servicio, fecha y período que cubre.',rule:'Devengo, asociación de ingresos y gastos y soporte documental.'},
 asset:{title:'Compra de activo fijo',debit:'Activo fijo específico',credit:'Proveedor o medio de pago',explain:'Un equipo que dará beneficios durante varios períodos puede ser un activo. Debe reconoce su costo y Haber el pago o la deuda. La depreciación se registra aparte con vida útil y valor residual revisados.',support:'Factura y descripción del activo, fecha disponible para uso.',rule:'Distingue inversión de gasto y distribuye su consumo durante la vida útil.'},
 sale:{title:'Venta y costo de la mercancía',debit:'Caja, banco o cliente',credit:'Ingresos por ventas',explain:'La venta registra dinero o cuenta por cobrar contra ingresos. Además, el costo real de la mercancía vendida va al Debe de costo de ventas y al Haber de inventarios. Precio de venta y costo son importes distintos.',support:'Factura de venta y costo real de los productos vendidos.',rule:'Reconocimiento del ingreso y del costo relacionado (NIC 2).'},
 supplier:{title:'Pago de deuda a proveedor',debit:'Proveedores / deuda',credit:'Caja o banco',explain:'El gasto o la compra ya se reconoció antes. Este pago reduce la deuda en el Debe y el dinero en el Haber; no vuelve a registrar la compra.',support:'Comprobante de pago y referencia de la factura pendiente.',rule:'Evita duplicar gastos o inventario cuando pagas una deuda.'},
 collection:{title:'Cobro de una factura pendiente',debit:'Caja o banco',credit:'Cuenta por cobrar',explain:'El cobro aumenta dinero y reduce la cuenta por cobrar. El ingreso por venta ya se registró con la factura; no debe reconocerse otra vez.',support:'Recibo de cobro y factura del cliente.',rule:'Distingue venta de cobro y conserva el saldo por cliente.'},
 transfer:{title:'Depósito de caja a banco',debit:'Banco que recibe',credit:'Caja que entrega',explain:'Solo cambia dónde está el dinero: aumenta banco y disminuye caja. No genera ventas ni gasto. Las comisiones se registran por separado.',support:'Boleta o confirmación del depósito y cierre de caja.',rule:'Movimiento entre activos; no altera la utilidad.'},
 capital:{title:'Aporte del propietario',debit:'Caja o banco',credit:'Aportación de capital',explain:'El aporte aumenta dinero y patrimonio. No es una venta ni ingreso operativo. Identifica al propietario y conserva el soporte del aporte.',support:'Recibo o acuerdo del aporte y evidencia del depósito.',rule:'Entidad económica: separa aportes del propietario de las ventas.'},
 loan:{title:'Préstamo recibido',debit:'Caja o banco',credit:'Préstamo por pagar',explain:'El desembolso recibido aumenta dinero y una deuda. No es ingreso por ventas. Intereses y comisiones requieren su registro separado; los pagos de capital reducen el pasivo.',support:'Contrato, desembolso y calendario de pagos.',rule:'Distingue financiamiento, capital e intereses.'},
 shortage:{title:'Pérdida o faltante de inventario',debit:'Gasto o pérdida de inventario',credit:'Inventarios',explain:'Reduce inventarios por su costo y reconoce la pérdida. Verifica el conteo, la causa y la autorización; el asiento contable no modifica las unidades físicas del sistema.',support:'Acta de conteo y ajuste de inventario aprobado.',rule:'Prudencia, trazabilidad y valoración al costo.'},
 depreciation:{title:'Depreciación del período',debit:'Gasto de depreciación',credit:'Depreciación acumulada',explain:'Reconoce el consumo del activo: Debe al gasto y Haber a la depreciación acumulada. Aquí ingresas el importe ya calculado; no se inventa una tasa ni una vida útil fiscal.',support:'Ficha del activo y cálculo aprobado del período.',rule:'Distribución sistemática del costo del activo durante su vida útil.'},
 quota:{title:'Pago de cuota fija',debit:'Gasto de tributos / cuota fija',credit:'Caja o banco',explain:'Registra el importe de la cuota efectivamente notificada o pagada a la DGI. No se calcula aquí la tarifa fiscal. Si ya reconociste una deuda por ese mismo tributo, usa la cuenta por pagar para no duplicar gasto.',support:'BIT, recibo DGI y período pagado.',rule:'Separa el tributo del régimen simplificado de la utilidad contable.'},
 advance:{title:'Anticipo a proveedor',debit:'Anticipos a proveedores',credit:'Caja o banco',explain:'El anticipo aún no es gasto ni inventario recibido. Debe registra el derecho frente al proveedor y Haber la salida de dinero. Al recibir la factura se aplica el anticipo.',support:'Pago, orden de compra y proveedor beneficiario.',rule:'Devengo: pagar antes no implica consumir o recibir aún.'}
};
function cents(value){const number=root.AccountingNumber?root.AccountingNumber.parse(value):Number(String(value).replace(/,/g,''));if(!Number.isFinite(number)||number<0||number>999999999)return null;const rounded=Math.round((number+Number.EPSILON)*100);return Math.abs(number*100-rounded)<0.0001?rounded:null;}
function propose(input){
 const lesson=lessons[input.operation];if(!lesson)throw Error('Selecciona una operación.');
 const total=cents(input.total);if(!total)throw Error('Escribe un importe mayor que cero, con máximo dos decimales.');
 if(!input.debit||!input.credit||input.debit===input.credit)throw Error('Revisa las cuentas: Debe y Haber deben ser diferentes.');
 const lines=[],add=(account,debit,credit,detail)=>lines.push({cuenta_id:account,debe:debit/100,haber:credit/100,descripcion:detail});
 add(input.debit,total,0,lesson.title);
 if(['inventory','production_inventory','expense','asset'].includes(input.operation)&&input.payment==='partial'){
  const paid=cents(input.paid);if(paid===null||paid<=0||paid>=total)throw Error('El pago parcial debe ser mayor que cero y menor al total.');
  if(!input.payable||[input.debit,input.credit].includes(input.payable))throw Error('Selecciona una cuenta por pagar distinta de la cuenta del pago y de la compra.');
  add(input.credit,0,paid,'Pago parcial');add(input.payable,0,total-paid,'Saldo pendiente al proveedor');
 }else add(input.credit,0,total,lesson.title);
 if(input.operation==='sale'){
  const cost=cents(input.cost);if(!cost)throw Error('Para esta venta indica el costo real de los productos vendidos.');
  if(!input.costAccount||!input.inventoryAccount||input.costAccount===input.inventoryAccount||[input.debit,input.credit].includes(input.costAccount)||[input.debit,input.credit].includes(input.inventoryAccount))throw Error('Revisa las cuentas separadas de costo de ventas e inventarios.');
  add(input.costAccount,cost,0,'Costo de mercancía vendida');add(input.inventoryAccount,0,cost,'Salida de inventario al costo');
 }
 return lines;
}
function classify(value){
 const text=String(value).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
 const matches=[];
 const rules={inventory:/compr[ae].*(paca|mercanc|inventario|ropa|producto)|ingreso.*mercanc/,expense:/alquiler|energia|electricidad|agua|internet|gasto|servicio/,asset:/compr[ae].*(equipo|vehiculo|computadora|mobiliario)/,sale:/\bventa\b|vendi|vendimos/,supplier:/pag.*(proveedor|deuda.*factura)/,collection:/cobr.*(cliente|factura|pendiente)/,transfer:/deposit.*(caja|banco)|traspas.*banco/,capital:/aport.*(capital|propietario)/,loan:/prestamo.*(recib|desembolso)/,shortage:/faltante|merma|perdida.*inventario/,depreciation:/depreciacion/,quota:/cuota fija|\bbit\b.*dgi/,advance:/anticipo.*proveedor/};
 for(const [operation,regex] of Object.entries(rules))if(regex.test(text))matches.push(operation);
 return matches.length===1?matches[0]:null;
}
const api={lessons,cents,propose,classify};if(typeof module!=='undefined'&&module.exports)module.exports=api;root.AccountingSchool=api;
})(typeof window!=='undefined'?window:globalThis);
