/* Mobile presentation reuses the existing form, prices, draft and product creation. */
document.addEventListener('DOMContentLoaded', () => {
  const form=document.getElementById('ingreso-form');if(!form)return;
  const media=matchMedia('(max-width:767.98px)');let step=1;
  const byId=id=>document.getElementById(id);
  const meta=form.querySelector('.odoo-modal-section');
  const products=form.querySelector('.search-panel');
  const productGroup=products.closest('.row.g-4');
  const totals=form.querySelector('.totals-bar');
  const footer=form.lastElementChild;
  meta.dataset.mobileSteps='1';products.dataset.mobileSteps='2 3';products.children[0].dataset.mobileSteps='2';products.children[1].dataset.mobileSteps='2';productGroup.dataset.mobileSteps='2 3';totals.dataset.mobileSteps='3';footer.dataset.mobileSteps='3';
  totals.nextElementSibling.dataset.mobileSteps='3';
  const steps=document.createElement('nav');steps.className='hp-entry-mobile-only hp-mobile-steps';steps.setAttribute('aria-label','Pasos del ingreso');
  steps.innerHTML='<button type="button" data-step="1">1. Datos</button><button type="button" data-step="2">2. Productos</button><button type="button" data-step="3">3. Revisar</button>';
  const guide=document.createElement('p');guide.className='hp-entry-mobile-only hp-mobile-guide';guide.setAttribute('aria-live','polite');
  form.prepend(guide);form.prepend(steps);
  const review=document.createElement('div');review.className='hp-entry-mobile-only hp-mobile-review';review.dataset.mobileSteps='3';productGroup.before(review);
  const actions=document.createElement('div');actions.className='hp-entry-mobile-only hp-mobile-actions';
  actions.innerHTML='<button type="button" class="btn btn-outline-secondary" data-back>Volver</button><button type="button" class="btn btn-primary" data-next>Continuar</button>';
  form.after(actions);
  function warn(message){if(window.Swal)Swal.fire({icon:'warning',title:'Revisa el ingreso',text:message});else alert(message);}
  function validMeta(){
    const missing=[];
    if(!byId('field-tipo').value)missing.push('tipo de ingreso');
    if(!byId('field-fecha').value)missing.push('fecha');
    if(!form.querySelector('[name=bodega_id]').value)missing.push('bodega');
    const option=byId('field-tipo').selectedOptions[0];
    const name=(option?.textContent||'').toLowerCase();
    const requires=option?.dataset.requiereProveedor==='1'||name.includes('compra')||name.includes('import');
    if(requires&&!byId('field-proveedor').value)missing.push('proveedor');
    if(missing.length){warn('Completa: '+missing.join(', ')+'.');return false;}return true;
  }
  function show(next){
    if(next>1&&!validMeta())return;
    if(next===3&&!byId('items-table').querySelector('[data-item-row]')){warn('Agrega un producto existente o crea uno nuevo antes de revisar.');return;}
    step=next;render();form.scrollIntoView({behavior:'smooth',block:'start'});
  }
  function render(){
    document.body.classList.toggle('hp-entry-mobile',media.matches);
    actions.classList.toggle('hp-mobile-hidden',byId('ingreso-form-section').classList.contains('hidden'));
    form.querySelectorAll('[data-mobile-steps]').forEach(el=>el.classList.toggle('hp-mobile-hidden',media.matches&&!el.dataset.mobileSteps.split(' ').includes(String(step))));
    steps.querySelectorAll('button').forEach(button=>{if(Number(button.dataset.step)===step)button.setAttribute('aria-current','step');else button.removeAttribute('aria-current');});
    guide.textContent=['Elige dónde recibirás la mercadería y completa los datos del ingreso.','Busca por código o nombre. Si no existe, pulsa Nuevo producto rápido.','Revisa cantidades, costos y precios. Puedes corregirlos antes de registrar.'][step-1];
    actions.querySelector('[data-back]').disabled=step===1;
    actions.querySelector('[data-next]').textContent=step===3?'Registrar ingreso':step===2?'Revisar ingreso':'Agregar productos';
    review.replaceChildren();
    for(const [label,field] of [['Bodega',form.querySelector('[name=bodega_id]')],['Tipo',byId('field-tipo')],['Proveedor',byId('field-proveedor')],['Fecha',byId('field-fecha')],['Moneda',byId('field-moneda')]]){
      const line=document.createElement('p');line.textContent=label+': '+(field.tagName==='SELECT'?field.selectedOptions[0]?.textContent.trim()||'Sin seleccionar':field.value);review.append(line);
    }
  }
  steps.addEventListener('click',event=>{const button=event.target.closest('[data-step]');if(button)show(Number(button.dataset.step));});
  actions.querySelector('[data-back]').addEventListener('click',()=>show(Math.max(1,step-1)));
  actions.querySelector('[data-next]').addEventListener('click',()=>{if(step<3)show(step+1);else form.requestSubmit();});
  form.addEventListener('submit',event=>{if(media.matches&&event.defaultPrevented){step=byId('ingreso-client-error')?.textContent.startsWith('Faltan:')?1:2;render();form.scrollIntoView({behavior:'smooth'});}});
  media.addEventListener('change',render);
  new MutationObserver(render).observe(byId('ingreso-form-section'),{attributes:true,attributeFilter:['class']});
  function labelCards(){
    for(const [id,labels] of [['product-list',['','', 'Costo','Precio de venta','Existencia','Cantidad','']],['items-table',['','Cantidad','Costo unitario','Precio de venta','Subtotal','']]]){
      byId(id).querySelectorAll('tr').forEach(row=>Array.from(row.querySelectorAll('td')).forEach((td,index)=>{if(labels[index])td.dataset.label=labels[index];}));
    }
  }
  new MutationObserver(labelCards).observe(byId('items-table'),{childList:true,subtree:true});
  new MutationObserver(labelCards).observe(byId('product-list'),{childList:true});
  const quick=byId('quick-product-form');
  const advanced=document.createElement('details');advanced.className='hp-mobile-advanced';advanced.innerHTML='<summary>Clasificación y opciones adicionales</summary><div class="row g-3"></div>';
  const quickRow=quick.querySelector('.row.g-3');
  const extraIds=['quick-product-linea','quick-product-segmento','quick-product-marca','quick-product-margin'];
  const originalPlaces=extraIds.map(id=>{const el=byId(id)?.closest('.col-md-6');return el?{el,parent:el.parentElement,next:el.nextSibling}:null;}).filter(Boolean);
  function moveExtra(){
    if(media.matches){quickRow.append(advanced);originalPlaces.forEach(({el})=>advanced.querySelector('.row').append(el));}
    else{originalPlaces.slice().reverse().forEach(({el,parent,next})=>parent.insertBefore(el,next&&next.parentElement===parent?next:null));advanced.remove();}
  }
  media.addEventListener('change',moveExtra);
  labelCards();moveExtra();render();
});
