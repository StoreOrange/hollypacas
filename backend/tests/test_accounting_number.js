const assert=require('node:assert/strict');
const {parse,format}=require('../app/static/js/accounting-number.js');
assert.equal(parse('1,234,567.89'),1234567.89);assert.equal(parse('-1,250.75'),-1250.75);
assert.equal(parse('1250.75'),1250.75);assert.equal(parse(''),0);assert.equal(parse('.50'),.5);
for(const value of ['1,23','1.000,25','abc','NaN','Infinity'])assert.ok(Number.isNaN(parse(value)));
assert.equal(format(1234567.8),'1,234,567.80');assert.equal(format(36.6243,4),'36.6243');
const school=require('../app/static/js/accounting-school.js');assert.equal(school.cents('12,345.67'),1234567);
console.log('PASS: separadores de miles, negativos, decimales, tasas, rechazo de formatos ambiguos y cálculo en centavos.');
