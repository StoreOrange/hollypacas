(() => {
  'use strict';
  const form=document.getElementById('mobileExpenseForm');if(!form)return;
  const data=JSON.parse(document.getElementById('mobileExpenseData').textContent);
  const field=id=>document.getElementById(id);
  const category=field('expenseCategory'),payment=field('expensePayment'),branch=field('expenseBranch');
  const warehouse=field('expenseWarehouse'),reference=field('expenseReference'),save=field('expenseSave');
  const categoryIcons={extraordinary:'bi-exclamation-circle',transport:'bi-truck',fuel:'bi-fuel-pump',repair:'bi-tools',cleaning:'bi-droplet',supplies:'bi-box-seam',electricity:'bi-lightning-charge',water:'bi-droplet-half',internet:'bi-wifi',stationery:'bi-pencil-square',rent:'bi-building',professional:'bi-person-workspace',security:'bi-shield-check',advertising:'bi-megaphone',bank_fees:'bi-bank',other:'bi-three-dots'};
  const setIcon=(id,icon)=>{field(id).className='bi '+icon;};
  function saveLabel(text,icon){field('expenseSaveText').textContent=text;setIcon('expenseSaveIcon',icon+' me-1');}
  let reviewed=false,sending=false;
  const label=select=>select.selectedOptions[0]?.textContent||'';
  function invalidate(){reviewed=false;field('expenseConfirmation').hidden=true;saveLabel('Revisar y registrar gasto','bi-clipboard-check');}
  function update(){
    const selected=data.categories.find(c=>c.value===category.value);
    setIcon('expenseCategoryIcon',categoryIcons[category.value]||'bi-tags');
    setIcon('expenseCurrencyIcon',field('expenseCurrency').value==='USD'?'bi-currency-dollar':'bi-coin');
    field('expenseCategoryHint').textContent=selected?.hint||'Selecciona el tipo; el sistema elegirá la cuenta correspondiente.';
    field('expenseAccountHint').textContent=selected?'Cuenta sugerida: '+selected.account:'Selecciona el tipo de gasto.';
    [...payment.options].forEach(option=>{option.hidden=!!option.dataset.branch&&option.dataset.branch!==branch.value;option.disabled=option.hidden;});
    if(payment.selectedOptions[0]?.disabled)payment.value='';
    [...warehouse.options].forEach(option=>{option.hidden=!!option.value&&option.dataset.branch!==branch.value;option.disabled=option.hidden;});
    if(warehouse.selectedOptions[0]?.disabled)warehouse.value='';
    const kind=payment.selectedOptions[0]?.dataset.kind;
    setIcon('expensePaymentIcon',({cash:'bi-cash-stack',bank:'bi-bank',personal:'bi-person-badge'})[kind]||'bi-wallet2');
    field('expenseWarehouseWrap').hidden=kind!=='cash';warehouse.required=kind==='cash';warehouse.disabled=kind!=='cash';
    field('expensePaidByWrap').hidden=kind!=='personal';field('expensePaidBy').required=kind==='personal';field('expensePaidBy').disabled=kind!=='personal';
    field('expensePaymentHint').textContent=({cash:'Este gasto se descontará una vez de la caja seleccionada y quedará en el cierre.',bank:'Se registrará el pago del banco. No se descuenta de la caja de ventas.',personal:'Quedará un reembolso pendiente para quien puso su dinero. No se descuenta de caja ni banco.'})[kind]||'Selecciona el origen real del dinero para preparar el registro.';
    field('expenseRateHint').hidden=field('expenseCurrency').value!=='USD';
    reference.required=!field('expenseNoSupport').checked;
  }
  function suggest(){
    const text=field('expenseDescription').value.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
    const rules={fuel:/combustible|gasolina|diesel/,transport:/transporte|flete|pasaje|taxi/,repair:/reparacion|mantenimiento|arreglo/,cleaning:/limpieza|jabon|cloro/,electricity:/energia|electricidad|luz electrica/,water:/agua potable/,internet:/internet|telefono|telefonia/,stationery:/papeleria|lapicero|utiles de oficina/,rent:/alquiler|arrendamiento/,professional:/honorario|servicio profesional/,security:/vigilancia|seguridad/,advertising:/publicidad|promocion/,bank_fees:/comision bancaria|cargo bancario/,supplies:/insumo/};
    const matches=Object.entries(rules).filter(([,rule])=>rule.test(text)).map(([key])=>key);
    if(matches.length===1&&data.categories.some(c=>c.value===matches[0])){category.value=matches[0];invalidate();update();category.focus();}
    else{field('expenseCategoryHint').textContent='Selecciona el tipo de gasto para confirmar cómo registrarlo. Para un imprevisto puedes usar Gasto extraordinario.';category.focus();}
  }
  field('suggestExpenseType').addEventListener('click',suggest);
  form.addEventListener('input',invalidate);form.addEventListener('change',()=>{invalidate();update();});
  form.addEventListener('keydown',event=>{
    if(event.key!=='Enter'||event.isComposing||!event.target.matches('input:not([type="hidden"]),select'))return;
    event.preventDefault();if(!event.target.checkValidity()){event.target.reportValidity();return;}
    const fields=[...form.querySelectorAll('input:not([type="hidden"]),textarea,select,button[type="submit"]')].filter(el=>!el.disabled&&el.getClientRects().length&&!el.closest('[hidden]'));
    const next=fields[fields.indexOf(event.target)+(event.shiftKey?-1:1)];next?.focus();
  });
  form.addEventListener('submit',event=>{
    if(sending){event.preventDefault();return;}
    if(!reviewed){
      event.preventDefault();
      field('expenseConfirmation').textContent='Revisa antes de confirmar:\n'+label(category)+'\n'+field('expenseDescription').value+'\nMonto: '+label(field('expenseCurrency'))+' '+field('expenseAmount').value+'\nFecha: '+field('expenseDate').value+' · '+label(branch)+'\nOrigen: '+label(payment)+(payment.value.startsWith('cash:')?' · '+label(warehouse):'')+'\nRecibió: '+field('expenseBeneficiary').value+(payment.value==='personal'?'\nPor reembolsar a: '+field('expensePaidBy').value:'')+'\nSoporte: '+(reference.value.trim()||'Pendiente');
      const reviewIcon=document.createElement('i');reviewIcon.className='bi bi-clipboard-check';reviewIcon.setAttribute('aria-hidden','true');field('expenseConfirmation').prepend(reviewIcon);
      field('expenseConfirmation').hidden=false;saveLabel('Confirmar y generar recibo','bi-file-earmark-check');reviewed=true;save.focus();return;
    }
    sending=true;save.disabled=true;saveLabel('Registrando…','bi-hourglass-split');
  });
  const modal=field('voucherPreviewModal'),viewer=window.AccountingVoucherPreview(modal,{basePath:'/m/gastos/'});
  document.querySelectorAll('[data-voucher-preview]').forEach(link=>link.addEventListener('click',event=>{event.preventDefault();viewer.open(link.dataset.voucherPreview);}));
  modal.addEventListener('hidden.bs.modal',()=>{const url=new URL(location.href);url.searchParams.delete('saved');history.replaceState(null,'',url);field('expenseDescription').focus();});
  update();if(data.saved)viewer.open(data.saved);
})();
