(function(root){
'use strict';
function parse(value){
 const text=String(value??'').trim();if(!text)return 0;
 if(!/^-?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d*)?$/.test(text)&&!/^[-]?\.\d+$/.test(text))return NaN;
 return Number(text.replace(/,/g,''));
}
function format(value,decimals=2){return new Intl.NumberFormat('en-US',{minimumFractionDigits:decimals,maximumFractionDigits:decimals}).format(Number(value));}
function bind(input){
 if(input.dataset.accountingNumber)return;
 const step=input.getAttribute('step')||'.01';const decimals=step.includes('.')?step.split('.')[1].length:2;
 input.dataset.accountingNumber=String(decimals);input.type='text';input.inputMode='decimal';input.autocomplete='off';
 if(input.value&&Number.isFinite(parse(input.value)))input.value=format(parse(input.value),decimals);
}
function isField(input){return input instanceof HTMLInputElement&&input.closest('#layout')&&(input.dataset.accountingNumber||input.matches('input[type="number"][step*="."]'));}
function validate(input,editing=false){
 const decimals=Number(input.dataset.accountingNumber),raw=input.value.replace(/,/g,'');const value=parse(editing?raw:input.value);
 const minimum=input.getAttribute('min'),maximum=input.getAttribute('max');
 const valid=(!raw||Number.isFinite(value))&&(!raw.includes('.')||raw.split('.')[1].length<=decimals)&&(minimum===null||value>=Number(minimum)||!raw)&&(maximum===null||value<=Number(maximum)||!raw);
 input.setCustomValidity(valid?'':`Escribe un monto válido, con máximo ${decimals} decimales${minimum!==null?' y mínimo '+minimum:''}.`);return valid;
}
if(typeof document!=='undefined'){
 const scan=()=>document.querySelectorAll('#layout input[type="number"][step*="."]').forEach(bind);
 document.addEventListener('input',event=>{
  const input=event.target;if(!isField(input))return;if(!input.dataset.accountingNumber)bind(input);
  if(!validate(input,true)||!input.value)return;
  const caret=input.selectionStart??input.value.length;const significant=input.value.slice(0,caret).replace(/,/g,'').length;
  const raw=input.value.replace(/,/g,'');const [integer,fraction]=raw.split('.');
  const formatted=integer.replace(/\B(?=(\d{3})+(?!\d))/g,',')+(fraction!==undefined?'.'+fraction:'');
  input.value=formatted;let position=0,count=0;while(position<formatted.length&&count<significant){if(formatted[position]!==',')count++;position++;}input.setSelectionRange(position,position);
 },true);
 document.addEventListener('blur',event=>{const input=event.target;if(!isField(input))return;if(!input.dataset.accountingNumber)bind(input);if(validate(input)&&input.value)input.value=format(parse(input.value),Number(input.dataset.accountingNumber));},true);
 document.addEventListener('submit',event=>{
  const form=event.target;if(!(form instanceof HTMLFormElement)||!form.closest('#layout'))return;
  const fields=[...form.querySelectorAll('input[data-accounting-number][name]')];
  for(const input of fields){if(!validate(input)){event.preventDefault();input.reportValidity();return;}}
  // Hidden canonical fields keep presentation commas out of backend decimals.
  form.querySelectorAll('[data-accounting-canonical]').forEach(input=>input.remove());
  for(const input of fields){const hidden=document.createElement('input');hidden.type='hidden';hidden.name=input.name;hidden.value=input.value?String(parse(input.value)):'';hidden.dataset.accountingCanonical='1';input.dataset.accountingFieldName=input.name;input.removeAttribute('name');form.appendChild(hidden);}
  setTimeout(()=>fields.forEach(input=>{if(input.dataset.accountingFieldName)input.name=input.dataset.accountingFieldName;}),0);
 },true);
 scan();if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',scan);
 new MutationObserver(scan).observe(document.documentElement,{childList:true,subtree:true});
}
const api={parse,format,bind};if(typeof module!=='undefined'&&module.exports)module.exports=api;root.AccountingNumber=api;
})(typeof window!=='undefined'?window:globalThis);
