(() => {
  'use strict';
  const root=document.querySelector('.shoe-prices');if(!root)return;
  const $=id=>document.getElementById(id), labels=JSON.parse($('shoePriceLabels').textContent);
  const inputs=Array.from(root.querySelectorAll('[data-price-tier]'));
  const money=value=>new Intl.NumberFormat('es-NI',{minimumFractionDigits:2,maximumFractionDigits:2}).format(value);
  const qty=value=>new Intl.NumberFormat('es-NI',{maximumFractionDigits:2}).format(value);
  const el=(tag,cls,text)=>{const node=document.createElement(tag);if(cls)node.className=cls;if(text!==undefined)node.textContent=text;return node;};
  const icon=name=>{const n=el('i','bi '+name);n.setAttribute('aria-hidden','true');return n;};
  let groups=[],selected=new Set(),expanded=new Set(),controller=null,searchVersion=0,saving=false,pending=null;
  function status(text,kind=''){ $('spStatus').textContent=text;$('spStatus').className='sp-status '+kind; }
  function prices(){return inputs.map(input=>input.value.trim()===''||!input.validity.valid?null:Number(input.value));}
  function validPrices(){const values=prices();return values.every(n=>n!==null&&Number.isFinite(n)&&n>=0&&n<=9999999999.99)?values:null;}
  function selection(){ $('spSelectionCount').textContent=selected.size+' producto'+(selected.size===1?' seleccionado':'s seleccionados');$('spReview').disabled=!selected.size||saving;const all=$('spCheckAll');all.disabled=!groups.length||saving;all.checked=groups.length>0&&selected.size===groups.length;all.indeterminate=selected.size>0&&selected.size<groups.length; }
  function showEmpty(title,description,ico='bi-search'){
    const td=el('td');td.colSpan=7;const block=el('div','sp-empty');block.append(icon(ico),el('strong','',title),el('span','',description));td.append(block);const tr=el('tr');tr.append(td);$('spRows').replaceChildren(tr);selection();
  }
  function render(){
    if(!groups.length){showEmpty('Sin resultados','Prueba otro código, color o talla.','bi-box-seam');return;}
    const fragment=document.createDocumentFragment(),tier=Number($('spCompareTier').value),next=prices()[tier-1];
    for(const group of groups){
      const tr=el('tr',selected.has(group.id)?'sp-selected':'');tr.dataset.productId=group.id;
      const checkCell=el('td','sp-check-cell'),check=el('input','form-check-input');check.type='checkbox';check.checked=selected.has(group.id);check.disabled=saving;check.setAttribute('aria-label','Seleccionar '+group.code);
      check.addEventListener('change',()=>{if(check.checked)selected.add(group.id);else selected.delete(group.id);tr.classList.toggle('sp-selected',check.checked);selection();});checkCell.append(check);
      const product=el('td');product.append(el('div','sp-product-code',group.code),el('div','sp-product-name',group.description));
      const variants=el('td'),details=el('button','sp-variant-button');details.type='button';details.setAttribute('aria-expanded',String(expanded.has(group.id)));details.setAttribute('aria-controls','spVariants'+group.id);
      const colors=new Set(group.items.map(item=>item.color||'—')),sizes=new Set(group.items.map(item=>item.talla||'—'));
      details.append(icon('bi-palette'),el('span','',colors.size+' color'+(colors.size===1?'':'es')+' · '+sizes.size+' talla'+(sizes.size===1?'':'s')),icon(expanded.has(group.id)?'bi-chevron-up':'bi-chevron-down'));details.addEventListener('click',()=>{if(expanded.has(group.id))expanded.delete(group.id);else expanded.add(group.id);render();});variants.append(details);
      const stock=el('td','sp-number');stock.append(el('span','sp-stock',qty(group.items.reduce((sum,item)=>sum+Number(item.existencia_global||0),0))));
      const current=Number(group.items[0]['precio_venta'+tier]||0),before=el('td','sp-number',money(current)),after=el('td','sp-number');after.append(el('span','sp-new-value',next===null?'—':money(next)));
      const diff=el('td','sp-number sp-delta',next===null?'—':(next>current?'+':'')+money(next-current));tr.append(checkCell,product,variants,stock,before,after,diff);fragment.append(tr);
      if(expanded.has(group.id)){
        const row=el('tr','sp-details-row');row.id='spVariants'+group.id;const cell=el('td');cell.colSpan=7;const grid=el('div','sp-variant-grid');
        for(const item of [...group.items].sort((a,b)=>String(a.color).localeCompare(String(b.color))||String(a.talla).localeCompare(String(b.talla),undefined,{numeric:true}))){const chip=el('div','sp-variant-chip');chip.append(el('strong','',(item.color||'—')+' · Talla '+(item.talla||'—')),el('span','',(item.cod_variante||'')+' · '+qty(Number(item.existencia_global||0))+' uds.'));grid.append(chip);}cell.append(grid);row.append(cell);fragment.append(row);
      }
    }
    $('spRows').replaceChildren(fragment);selection();
  }
  async function search(event,chosenItem=null){
    event?.preventDefault();if(saving)return;stopSuggestions();
    const params=new URLSearchParams(),q=$('spQuery').value.trim(),color=$('spColor').value.trim(),size=$('spSize').value.trim();
    controller?.abort();const version=++searchVersion;groups=[];selected.clear();expanded.clear();pending=null;
    if(!q&&!color&&!size){showEmpty('Encuentra tus productos','Escribe un código, nombre, color o talla.');$('spResultsCount').textContent='Busca un producto para comenzar.';status('Escribe al menos un filtro.');return;}
    if(q)params.set('q',q);if(color)params.set('color',color);if(size)params.set('talla',size);params.set('limit','120');params.set('price_list',$('spCompareTier').value);
    controller=new AbortController();$('spSearch').disabled=true;$('spSearch').replaceChildren(icon('bi-hourglass-split'),document.createTextNode(' Buscando'));status('Buscando productos…');showEmpty('Buscando','Consultando variantes.','bi-hourglass-split');
    try{
      const response=await fetch('/sales/shoes/variants/search?'+params,{headers:{Accept:'application/json'},signal:controller.signal});const payload=await response.json();if(version!==searchVersion)return;
      if(!response.ok||!payload.ok||!Array.isArray(payload.items))throw Error(payload.message||'No se pudo realizar la búsqueda.');
      const map=new Map();for(const item of payload.items){const id=Number(item.producto_id);if(!Number.isSafeInteger(id)||id<=0)continue;if(!map.has(id))map.set(id,{id,code:String(item.cod_producto||''),description:String(item.descripcion||''),items:[]});map.get(id).items.push(item);}
      groups=[...map.values()].sort((a,b)=>a.code.localeCompare(b.code,undefined,{numeric:true}));if(chosenItem)expanded.add(Number(chosenItem.producto_id));render();$('spResultsCount').textContent=groups.length+' productos · '+payload.items.length+' variantes encontradas';status(payload.items.length>=120?'Se muestran hasta 120 variantes. Refina la búsqueda si necesitas más resultados.':'Selecciona los productos que quieras actualizar.');
    }catch(error){if(error.name==='AbortError'||version!==searchVersion)return;showEmpty('No se pudo buscar','Vuelve a intentarlo.','bi-exclamation-circle');$('spResultsCount').textContent='Búsqueda no completada.';status(error.message||'Error de conexión.','error');}
    finally{if(version===searchVersion){$('spSearch').disabled=false;$('spSearch').replaceChildren(icon('bi-search'),document.createTextNode(' Buscar'));}}
  }
  // Suggestions are read-only; scanner Enter keeps the explicit search flow.
  let suggestionTimer=null,suggestionController=null,suggestionVersion=0,suggestionItems=[],suggestionKey='',activeSuggestion=-1;
  const suggestionBox=$('spSuggestions'),suggestionList=$('spSuggestionsList'),queryInput=$('spQuery');
  function filterKey(){return [$('spQuery').value.trim(),$('spColor').value.trim(),$('spSize').value.trim(),$('spCompareTier').value].join('\u0000');}
  function stopSuggestions(){
    clearTimeout(suggestionTimer);suggestionController?.abort();suggestionVersion++;
    suggestionItems=[];suggestionKey='';activeSuggestion=-1;suggestionBox.hidden=true;
    queryInput.setAttribute('aria-expanded','false');queryInput.removeAttribute('aria-activedescendant');
  }
  function revealSuggestions(message){suggestionBox.hidden=false;queryInput.setAttribute('aria-expanded','true');$('spSuggestionStatus').textContent=message;}
  function highlightSuggestion(index){
    activeSuggestion=index;
    const options=suggestionList.querySelectorAll('[role="option"]');
    options.forEach((option,i)=>{option.classList.toggle('is-active',i===index);option.setAttribute('aria-selected',String(i===index));});
    if(index>=0&&options[index]){queryInput.setAttribute('aria-activedescendant',options[index].id);options[index].scrollIntoView({block:'nearest'});}
    else queryInput.removeAttribute('aria-activedescendant');
  }
  function chooseSuggestion(item){
    if(saving)return;
    queryInput.value=String(item.scan_code||item.cod_variante||item.cod_producto||'');
    stopSuggestions();search(null,item);
  }
  async function loadSuggestions(){
    if(saving)return;
    const q=queryInput.value.trim(),color=$('spColor').value.trim(),size=$('spSize').value.trim();
    if(q.length<2&&!color&&!size)return;
    const key=filterKey(),version=++suggestionVersion;
    suggestionController=new AbortController();suggestionList.replaceChildren();revealSuggestions('Buscando opciones…');
    const params=new URLSearchParams({limit:'12',price_list:$('spCompareTier').value});
    if(q)params.set('q',q);if(color)params.set('color',color);if(size)params.set('talla',size);
    try{
      const response=await fetch('/sales/shoes/variants/search?'+params,{headers:{Accept:'application/json'},signal:suggestionController.signal});
      const payload=await response.json();if(version!==suggestionVersion||key!==filterKey())return;
      if(!response.ok||!payload.ok||!Array.isArray(payload.items))throw Error('No se pudieron cargar las opciones.');
      suggestionItems=payload.items.slice(0,12);suggestionKey=key;activeSuggestion=-1;
      const fragment=document.createDocumentFragment();
      for(const [index,item] of suggestionItems.entries()){
        const option=el('button','sp-suggestion');option.type='button';option.id='spSuggestion'+index;option.setAttribute('role','option');option.setAttribute('aria-selected','false');option.tabIndex=-1;
        const code=el('span','sp-suggestion-code');code.append(el('strong','',String(item.cod_producto||'—')),el('small','',String(item.cod_variante||item.scan_code||'')));
        const detail=el('span','sp-suggestion-variant');detail.append(el('span','',String(item.color||'—')),el('small','', 'Talla '+String(item.talla||'—')));
        option.append(code,detail);
        for(const [warehouse,label] of [['central','Central'],['kg','KG'],['kgf','KGF']]){
          const count=Number(item.stock_bodegas?.[warehouse]??0),stock=el('span','sp-suggestion-stock'+(count>0?' has-stock':''));
          stock.append(el('small','',label),el('strong','',qty(Number.isFinite(count)?count:0)));option.append(stock);
        }
        option.setAttribute('aria-label',[item.cod_producto,item.cod_variante,item.color,'Talla '+(item.talla||'—'),...['central','kg','kgf'].map(key=>(key==='central'?'Central':key.toUpperCase())+': '+qty(Number(item.stock_bodegas?.[key]??0)))].filter(Boolean).join(', '));
        option.append(icon('bi-arrow-up-left'));
        option.addEventListener('pointerdown',event=>event.preventDefault());
        option.addEventListener('click',()=>chooseSuggestion(item));fragment.append(option);
      }
      suggestionList.replaceChildren(fragment);
      revealSuggestions(suggestionItems.length?suggestionItems.length+' opciones · ↑ ↓ para recorrer, Enter para elegir'+(payload.items.length>=12?' · Refina el filtro para ver más.':''):'Sin coincidencias. Prueba otro código, color o talla.');
    }catch(error){
      if(error.name==='AbortError'||version!==suggestionVersion)return;
      suggestionList.replaceChildren();revealSuggestions('No se pudieron cargar las opciones. Puedes usar Buscar para reintentar.');
    }
  }
  function scheduleSuggestions(){stopSuggestions();suggestionTimer=setTimeout(loadSuggestions,260);}
  for(const input of [queryInput,$('spColor'),$('spSize')])input.addEventListener('input',scheduleSuggestions);
  queryInput.addEventListener('focus',()=>{if(suggestionBox.hidden&&(queryInput.value.trim().length>=2||$('spColor').value.trim()||$('spSize').value.trim()))scheduleSuggestions();});
  queryInput.addEventListener('keydown',event=>{
    if(event.key==='Escape'){event.preventDefault();stopSuggestions();return;}
    if(event.key==='Tab'){stopSuggestions();return;}
    if(suggestionBox.hidden||!suggestionItems.length||suggestionKey!==filterKey())return;
    if(event.key==='ArrowDown'||event.key==='ArrowUp'){
      event.preventDefault();const next=activeSuggestion<0?(event.key==='ArrowDown'?0:suggestionItems.length-1):(activeSuggestion+(event.key==='ArrowDown'?1:-1)+suggestionItems.length)%suggestionItems.length;highlightSuggestion(next);
    }else if(event.key==='Enter'&&activeSuggestion>=0){event.preventDefault();chooseSuggestion(suggestionItems[activeSuggestion]);}
  });
  document.addEventListener('pointerdown',event=>{if(!suggestionBox.contains(event.target)&&!$('spSearchForm').contains(event.target))stopSuggestions();});
  $('spCompareTier').addEventListener('change',stopSuggestions);
  $('spSearchForm').addEventListener('submit',search);
  $('spCompareTier').addEventListener('change',()=>{if(groups.length)render();});
  for(const input of inputs)input.addEventListener('input',()=>{pending=null;if(groups.length)render();if(validPrices()&&$('spStatus').classList.contains('error'))status('Precios listos. Revisa la comparación antes de guardar.');});
  $('spCheckAll').addEventListener('change',event=>{selected=event.target.checked?new Set(groups.map(group=>group.id)):new Set();pending=null;render();});
  $('spClearPrices').addEventListener('click',()=>{if(saving)return;inputs.forEach(input=>input.value='');pending=null;if(groups.length)render();status('Nuevos precios limpiados. No se modificó ningún producto.');});
  $('spGenerate').addEventListener('click',()=>{
    const base=$('spBase'),step=$('spStep'),b=Number(base.value),s=Number(step.value);
    if(!base.value||!base.validity.valid||!step.value||!step.validity.valid||!Number.isFinite(b)||!Number.isFinite(s)||b<=0||s<0||b>9999999999.99){status('Indica un precio base válido y una rebaja de cero o mayor.','error');return;}
    const cents=Math.round(b*100),rebate=Math.round(s*100);inputs.forEach((input,index)=>input.value=(Math.max(0,cents-rebate*index)/100).toFixed(2));pending=null;if(groups.length)render();status('Escala generada. Revisa los siete precios antes de guardar.');
  });
  $('spReview').addEventListener('click',()=>{
    if(saving||!selected.size)return;const values=validPrices();if(!values){status('Completa los siete precios con valores válidos de cero o mayores.','error');const invalid=inputs.find(input=>input.value===''||!input.validity.valid);invalid?.focus();return;}
    pending={ids:[...selected],values};$('spSave').disabled=true;$('spConfirmSummary').textContent='Vas a cambiar los precios de '+pending.ids.length+' producto'+(pending.ids.length===1?'.':'s.');
    const codes=groups.filter(group=>selected.has(group.id)).map(group=>group.code);$('spConfirmProducts').textContent=codes.slice(0,8).join('\n')+(codes.length>8?'\nY '+(codes.length-8)+' productos más.':'');
    $('spConfirmPrices').replaceChildren(...values.map((value,index)=>{const tr=el('tr');tr.append(el('td','',labels[index]),el('td','sp-number',money(value)));return tr;}));$('spZeroWarning').hidden=!values.some(value=>value===0);$('spConfirmError').hidden=true;bootstrap.Modal.getOrCreateInstance($('spConfirmModal')).show();
  });
  function prepareSuccess(payload,job){
    const complete=Array.isArray(payload.updated_products)&&payload.updated_products.length>0;
    const products=complete?payload.updated_products:job.products;
    const count=Number(payload.updated_count)||products.length;
    $('spSuccessCount').textContent=count+' producto'+(count===1?' actualizado':'s actualizados')+' · 7 listas de precios';
    const cards=products.map(product=>{
      const card=el('section','sp-success-product');
      card.append(el('h3','',String(product.cod_producto||'Producto')),el('p','sp-success-product-name',String(product.descripcion||'')));
      const variants=Array.isArray(product.variantes)?product.variantes:[];
      card.append(el('p','sp-success-variant-title',complete?'Tallas y colores del producto':'Tallas y colores consultados'));
      if(!variants.length){card.append(el('p','sp-success-no-variants','Sin variantes de color y talla registradas.'));return card;}
      const grid=el('div','sp-success-variants');
      for(const variant of variants){const chip=el('div','sp-success-variant');chip.append(icon('bi-palette'),el('span','',String(variant.color||'—')),el('strong','', 'Talla '+String(variant.talla||'—')));if(variant.cod_variante)chip.title=String(variant.cod_variante);grid.append(chip);}
      if(variants.length>8){const details=el('details');details.append(el('summary','', 'Ver '+variants.length+' variantes de talla y color'),grid);card.append(details);}else card.append(grid);
      return card;
    });
    $('spSuccessProducts').replaceChildren(...cards);
    $('spSuccessModal').querySelector('.sp-success-body').scrollTop=0;
  }
  $('spSave').addEventListener('click',async()=>{
    if(saving||!pending)return;const job={ids:[...pending.ids],values:[...pending.values],products:groups.filter(group=>pending.ids.includes(group.id)).map(group=>({producto_id:group.id,cod_producto:group.code,descripcion:group.description,variantes:group.items.map(item=>({cod_variante:item.cod_variante,color:item.color,talla:item.talla}))}))};saving=true;selection();$('spSave').disabled=true;$('spSave').replaceChildren(icon('bi-hourglass-split'),document.createTextNode(' Guardando…'));$('spConfirmError').hidden=true;
    const modal=$('spConfirmModal');modal.querySelectorAll('[data-bs-dismiss]').forEach(button=>button.disabled=true);
    try{
      const data=new FormData();data.set('product_ids',job.ids.join(','));job.values.forEach((value,index)=>data.set('precio_venta'+(index+1),value.toFixed(2)));
      const response=await fetch('/inventory/shoes/prices/apply',{method:'POST',headers:{Accept:'application/json','X-Requested-With':'fetch'},body:data});const payload=await response.json().catch(()=>null);if(!response.ok||!payload?.ok)throw Error(payload?.message||'No se pudo confirmar el guardado. Revisa el resultado antes de volver a intentar.');
      const confirmedIds=new Set(Array.isArray(payload.updated_products)?payload.updated_products.map(product=>Number(product.producto_id)):job.ids);
      for(const group of groups)if(confirmedIds.has(group.id))for(const item of group.items)job.values.forEach((value,index)=>item['precio_venta'+(index+1)]=value);
      prepareSuccess(payload,job);
      modal.addEventListener('hidden.bs.modal',()=>bootstrap.Modal.getOrCreateInstance($('spSuccessModal')).show(),{once:true});
      selected.clear();pending=null;saving=false;bootstrap.Modal.getInstance(modal).hide();render();status(payload.message||'Precios actualizados.','success');
    }catch(error){$('spConfirmError').textContent=error.message||'No se pudo guardar. Comprueba el resultado antes de volver a confirmar.';$('spConfirmError').hidden=false;}
    finally{saving=false;$('spSave').disabled=false;$('spSave').replaceChildren(icon('bi-floppy'),document.createTextNode(' Confirmar y guardar'));modal.querySelectorAll('[data-bs-dismiss]').forEach(button=>button.disabled=false);selection();}
  });
  $('spSuccessModal').addEventListener('hidden.bs.modal',()=>{queryInput.focus();queryInput.select();stopSuggestions();});
  $('spConfirmModal').addEventListener('shown.bs.modal',()=>{if(!saving)$('spSave').disabled=false;});
  $('spConfirmModal').addEventListener('hide.bs.modal',event=>{if(saving)event.preventDefault();});
})();
