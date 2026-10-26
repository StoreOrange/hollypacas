const assert=require('node:assert/strict');
const {lessons,propose,classify,cents}=require('../app/static/js/accounting-school.js');
const base={total:'1234.56',debit:'d',credit:'c',payment:'cash',cost:'300.25',costAccount:'cost',inventoryAccount:'inv'};
for(const operation of Object.keys(lessons)){
 const lines=propose({...base,operation});
 assert.equal(lines.reduce((s,l)=>s+Math.round(l.debe*100),0),lines.reduce((s,l)=>s+Math.round(l.haber*100),0));
 assert.ok(lines.every(l=>(l.debe>0)!==(l.haber>0)));
}
const split=propose({...base,operation:'inventory',payment:'partial',paid:'234.56',payable:'p'});
assert.equal(split.length,3);assert.equal(split[2].haber,1000);
assert.equal(propose({...base,operation:'sale'}).length,4);
for(const total of ['0','-1','NaN','Infinity','1.001'])assert.throws(()=>propose({...base,operation:'expense',total}));
assert.throws(()=>propose({...base,operation:'inventory',payment:'partial',paid:'1234.56',payable:'p'}));
assert.throws(()=>propose({...base,operation:'sale',cost:'0'}));
assert.throws(()=>propose({...base,operation:'sale',costAccount:'inv'}));
assert.throws(()=>propose({...base,operation:'sale',debit:'c'}));
assert.equal(cents('100.50'),10050);
assert.equal(classify('Pagué el alquiler de octubre'),'expense');
assert.equal(classify('Compré pacas a crédito'),'inventory');
assert.equal(classify('Pago de cuota fija DGI'),'quota');
assert.equal(classify('Compra de computadoras para oficina'),'asset');
assert.equal(classify('Compra de pacas y pago de internet'),null);
assert.equal(classify('Operación especial no descrita'),null);
console.log('PASS: 13 operaciones cuadradas, pago parcial exacto, venta+costos, importes inválidos y orientación sin clasificación ambigua.');
for(const payment of ['cash','bank','credit','partial']){
 const lines=propose({...base,operation:'production_inventory',debit:'wip',payment,paid:'234.56',payable:'supplier'});
 assert.equal(lines[0].cuenta_id,'wip');
 assert.equal(lines[0].debe,1234.56);
 assert.ok(lines.every(line=>line.cuenta_id!=='inv'&&line.cuenta_id!=='cost'));
 assert.equal(lines.reduce((sum,line)=>sum+Math.round(line.haber*100),0),123456);
 assert.equal(lines.length,payment==='partial'?3:2);
}
console.log('PASS: compras de producción al contado, banco, crédito y pago parcial sin cuenta de inventario general ni costo de venta.');
