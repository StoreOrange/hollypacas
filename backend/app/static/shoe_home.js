(() => {
  const root=document.getElementById('shoe-home');if(!root)return;
  const el=id=>document.getElementById(id),esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const render=data=>{
    el('sh-date').textContent=data.date;
    el('sh-status').textContent=`Actualizado a las ${data.updated} · Se actualiza cada 30 segundos`;
    el('sh-status').classList.remove('error');
    if(el('sh-sales')){
      el('sh-sales').innerHTML=data.stores.map(s=>`<article class="sh-store"><div class="sh-store-top"><h4>${esc(s.name)}</h4><span class="sh-dot"></span></div><div class="sh-money">C$ ${esc(s.sales)}</div><span class="sh-small">${s.invoices} factura(s) del día</span></article>`).join('')||'<p>No hay tiendas disponibles.</p>';
      el('sh-sales-total').textContent=`C$ ${data.totals.sales}`;
    }
    if(el('sh-cash')){
      const opened=new Set([...root.querySelectorAll('details[open]')].map(d=>d.dataset.store));
      el('sh-cash').innerHTML=data.stores.map(s=>`<article class="sh-store"><div class="sh-store-top"><h4>${esc(s.name)}</h4><span class="sh-pill ${s.closed?'':'pending'}">${s.closed?'Arqueo registrado':'Sin arqueo'}</span></div><span class="sh-small">Efectivo recibido en ventas · antes del vuelto</span><div class="sh-pair"><span>Córdobas</span><strong>C$ ${esc(s.cash_cs)}</strong></div><div class="sh-pair"><span>Dólares</span><strong>US$ ${esc(s.cash_usd)}</strong></div><div class="sh-count"><span class="sh-small">Efectivo contado · último arqueo de hoy</span><div class="sh-pair"><span>Córdobas</span><strong>C$ ${esc(s.counted_cs)}</strong></div><div class="sh-pair"><span>Dólares</span><strong>US$ ${esc(s.counted_usd)}</strong></div></div><details class="sh-movements" data-store="${s.id}" ${opened.has(String(s.id))?'open':''}><summary>Depósitos y otros cobros · ${s.movements.length}</summary><p class="sh-note">Cobros de ventas y registros de depósitos se muestran separados; pueden corresponder al mismo movimiento.</p><div class="sh-movement-list">${s.movements.map(m=>`<div class="sh-movement"><span>${esc(m.kind)}<br>${esc(m.reference)}</span><strong>${m.currency==='USD'?'US$':'C$'} ${esc(m.amount)}</strong></div>`).join('')||'<p class="sh-note">Sin movimientos registrados hoy.</p>'}</div></details></article>`).join('');
      el('sh-received-total').textContent=`C$ ${data.totals.cash_cs} · US$ ${data.totals.cash_usd}`;
      el('sh-cash-total').textContent=`C$ ${data.totals.counted_cs} · US$ ${data.totals.counted_usd}`;
    }
  };
  render(JSON.parse(el('shoe-home-data').textContent));
  let loading=false;
  const refresh=async()=>{
    if(loading)return;loading=true;el('sh-refresh').disabled=true;
    const timeoutController=new AbortController(),timeout=setTimeout(()=>timeoutController.abort(),15000);
    try{const response=await fetch('/home/zapatos/resumen',{signal:timeoutController.signal,headers:{Accept:'application/json'}});const data=await response.json();if(!response.ok||!data.ok)throw Error();render(data.data);}
    catch(_){el('sh-status').textContent='No se pudo actualizar. Se conservan los últimos datos; vuelve a intentar.';el('sh-status').classList.add('error');}
    finally{clearTimeout(timeout);loading=false;el('sh-refresh').disabled=false;}
  };
  el('sh-refresh').addEventListener('click',refresh);
  setInterval(()=>{if(!document.hidden)refresh();},30000);
  let queryVersion=0,controller;
  el('sh-inventory-form')?.addEventListener('submit',async event=>{
    event.preventDefault();const q=el('sh-code').value.trim(),version=++queryVersion;controller?.abort();controller=new AbortController();
    el('sh-inventory-results').replaceChildren();
    if(!q){el('sh-inventory-status').textContent='Escribe un código para consultar.';return;}
    el('sh-inventory-status').textContent='Consultando existencias…';
    try{
      const response=await fetch('/home/zapatos/inventario?q='+encodeURIComponent(q),{signal:controller.signal,headers:{Accept:'application/json'}});
      const data=await response.json();if(!response.ok||!data.ok)throw Error();if(version!==queryVersion)return;
      el('sh-inventory-status').textContent=data.items.length?`${data.items.length} variantes encontradas${data.more?' · Hay más resultados; escribe un código más específico.':''}`:'No hay coincidencias para ese código.';
      el('sh-inventory-results').innerHTML=data.items.map(i=>`<article class="sh-inventory-item"><h4>${esc(i.code)} · ${esc(i.description)}</h4><span class="sh-small">Modelo ${esc(i.model)} · ${esc(i.color)} · Talla ${esc(i.size)}</span><div class="sh-stocks">${i.stores.map(s=>`<span class="sh-stock">${esc(s.name)}<strong>${esc(s.qty)}</strong></span>`).join('')}</div></article>`).join('');
    }catch(error){if(error.name!=='AbortError'&&version===queryVersion)el('sh-inventory-status').textContent='No se pudieron consultar las existencias. Intenta nuevamente.';}
  });
})();
