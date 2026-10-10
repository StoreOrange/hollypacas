(() => {
  const $ = id => document.getElementById(id);
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const cash = value => 'C$ ' + Number(value).toLocaleString('es-NI', {minimumFractionDigits:2,maximumFractionDigits:2});
  let results = [], selected = null, historyPage = 1, historyMore = false, searching = 0;
  const feedback = (message, error = false) => { $('return-feedback').textContent = message; $('return-feedback').className = 'alert ' + (error ? 'alert-danger' : 'alert-success'); };
  async function api(url, options) {
    const response = await fetch(url, {...options, headers:{Accept:'application/json',...options?.headers}});
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.message || data.detail || 'No se pudo completar la operación.');
    return data;
  }
  async function search() {
    const generation = ++searching;
    $('search-status').textContent='Buscando…';
    try {
      const data = await api('/sales/devoluciones/facturas?' + new URLSearchParams(new FormData($('return-search'))));
      if (generation !== searching) return;
      results=data.items;
      $('search-status').textContent=data.more ? 'Se muestran 100 productos. Afina los filtros.' : `${results.length} productos encontrados.`;
      $('return-results').innerHTML=results.map((r,i)=>`<tr><td><strong>${esc(r.factura)}</strong><div class="small text-muted">${esc(r.fecha)} · ${esc(r.hora)}</div><div class="small">Total ${cash(r.total_factura_cs)}</div></td><td>${esc(r.cliente)}</td><td><strong>${esc(r.producto)}</strong><div class="small text-muted">${esc(r.codigo)} · ${esc(r.color)} · Talla ${esc(r.talla)}</div></td><td class="text-end">${cash(r.precio_cs)}<div class="small text-muted">Pagado ${cash(r.neto_unitario_cs)}</div></td><td class="text-end">${r.cantidad}</td><td class="text-end fw-semibold">${r.disponible}</td><td><button type="button" class="btn btn-outline-primary" data-return-index="${i}" ${r.elegible?'':'disabled'}>${r.elegible?'Gestionar devolución':'No disponible'}</button></td></tr>`).join('') || '<tr><td colspan="7" class="text-center text-muted py-5">No encontramos productos. Revisa la factura o los filtros.</td></tr>';
    } catch(e) { $('search-status').textContent=e.message; }
  }
  function preview() {
    if (!selected) return;
    const quantity=Number($('return-quantity').value);
    const amount=quantity===selected.disponible ? selected.remaining_cs : Math.round(selected.total_linea_cs * quantity / selected.cantidad *100)/100;
    $('return-amount').textContent=cash(amount || 0);
    const kind=$('return-form').elements.tipo.value;
    $('return-amount-label').textContent=kind==='CANJE'?'Anticipo para canje':'Efectivo a entregar al cliente';
  }
  $('return-results').addEventListener('click', event => {
    const button=event.target.closest('[data-return-index]'); if(!button) return;
    selected=results[Number(button.dataset.returnIndex)];
    $('return-form').reset();
    const cashChoice=$('return-form').querySelector('[name=tipo][value=DINERO]');
    cashChoice.disabled=selected.solo_canje;
    cashChoice.closest('label').title=selected.solo_canje?'Las compras pagadas con anticipo se devuelven por canje.':'';
    $('return-item-id').value=selected.item_id;
    $('return-quantity').max=selected.disponible;
    const generic=['consumidor final','cliente local'].includes(selected.cliente.toLowerCase());
    $('return-customer-name').value=generic?'':selected.cliente;
    $('return-customer-name').readOnly=!generic;
    $('return-selected').innerHTML=`<div class="d-flex justify-content-between gap-3 flex-wrap"><div><strong>${esc(selected.producto)}</strong><div>${esc(selected.color)} · Talla ${esc(selected.talla)} · ${esc(selected.codigo)}</div></div><div>Factura <strong>${esc(selected.factura)}</strong><div class="text-muted">${esc(selected.fecha)} · ${esc(selected.hora)}</div></div><div>Cliente <strong>${esc(selected.cliente)}</strong><div>${selected.disponible} unidades disponibles · Pagado ${cash(selected.neto_unitario_cs)} por unidad</div></div></div>`;
    $('return-editor').classList.remove('d-none');preview();$('return-editor').scrollIntoView({behavior:'smooth',block:'start'});$('return-quantity').focus();
  });
  $('return-form').addEventListener('input',preview);
  $('return-form').addEventListener('change',preview);
  $('cancel-return').addEventListener('click',()=>$('return-editor').classList.add('d-none'));
  $('return-search').addEventListener('submit',e=>{e.preventDefault();search();});
  let timer;
  ['filter-invoice','filter-customer','filter-date','filter-product'].forEach(id=>$(id).addEventListener('input',()=>{clearTimeout(timer);timer=setTimeout(search,450);}));
  $('clear-return-search').addEventListener('click',()=>{$('return-search').reset();search();});
  $('return-form').addEventListener('submit',async e=>{
    e.preventDefault();
    const kind=$('return-form').elements.tipo.value;
    const summary=`${$('return-quantity').value} unidad(es) de ${selected.producto}. ${kind==='CANJE'?'Crear anticipo por':'Registrar efectivo entregado por'} ${$('return-amount').textContent}.`;
    const answer=await Swal.fire({title:kind==='CANJE'?'Confirmar devolución por canje':'Confirmar entrega de dinero',text:summary,icon:'question',showCancelButton:true,confirmButtonText:kind==='CANJE'?'Crear anticipo':'Confirmar dinero entregado',cancelButtonText:'Revisar'});
    if(!answer.isConfirmed) return;
    const button=$('return-submit');button.disabled=true;
    try {
      const data=await api('/sales/devoluciones',{method:'POST',body:new FormData($('return-form'))});
      const r=data.devolucion;
      $('return-form').elements.operation_key.defaultValue=data.operation_key;
      $('return-form').elements.operation_key.value=data.operation_key;
      $('return-editor').classList.add('d-none');
      feedback(`${r.numero} registrada. ${r.tipo==='CANJE'?'Anticipo disponible para '+r.cliente+':':'Efectivo entregado:'} ${cash(r.monto_cs)}. ${r.tipo==='CANJE'?'En facturación, selecciona Anticipo para canjearlo.':'Egreso de caja '+r.recibo+'.'}`);
      historyPage=1;await Promise.all([history(),search()]);$('return-feedback').scrollIntoView({behavior:'smooth',block:'center'});
    } catch(error) {feedback(error.message,true);} finally {button.disabled=false;}
  });
  async function history() {
    try {
      const params=new URLSearchParams(new FormData($('history-filters')));params.set('page',historyPage);
      const data=await api('/sales/devoluciones/historial?'+params);
      historyMore=data.more;
      $('return-history').innerHTML=data.items.map(r=>`<tr><td><strong>${esc(r.numero)}</strong><div class="small">Factura ${esc(r.factura)}</div><div class="small text-muted">${esc(r.fecha)} · ${esc(r.hora)}</div></td><td><strong>${esc(r.cliente)}</strong><div>${esc(r.producto)}</div><div class="small text-muted">${esc(r.variante)}</div></td><td class="text-end">${r.cantidad} unidad(es)<div class="fw-semibold">${cash(r.monto_cs)}</div></td><td><span class="badge ${r.tipo==='DINERO'?'bg-secondary':r.factura_canje?'bg-success':'bg-primary'}">${esc(r.estado)}</span><div class="small mt-2">${r.factura_canje?'Factura '+esc(r.factura_canje)+' · '+esc(r.fecha_canje):r.recibo?'Egreso '+esc(r.recibo):'Disponible completo para canje'}</div>${r.usuario_canje?'<div class="small text-muted">Canjeado por '+esc(r.usuario_canje)+'</div>':''}</td><td>${esc(r.usuario)}<div class="small text-muted">${esc(r.bodega)}</div><div class="small">${esc(r.motivo)}</div></td></tr>`).join('')||'<tr><td colspan="5" class="text-center text-muted py-5">No hay devoluciones en este filtro.</td></tr>';
      $('history-status').textContent=`${data.total} devoluciones · Página ${historyPage}`;
      $('history-prev').disabled=historyPage===1;$('history-next').disabled=!historyMore;
    }catch(e){$('history-status').textContent=e.message;}
  }
  $('history-filters').addEventListener('submit',e=>{e.preventDefault();historyPage=1;history();});
  $('refresh-returns').addEventListener('click',history);
  $('history-prev').addEventListener('click',()=>{if(historyPage>1){historyPage--;history();}});
  $('history-next').addEventListener('click',()=>{if(historyMore){historyPage++;history();}});
  history();
})();
