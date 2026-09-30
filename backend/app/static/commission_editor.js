(() => {
  'use strict';
  const body = document.getElementById('assignment-body');
  const form = document.getElementById('assignment-form');
  if (!body || !form) return;
  const cfg = JSON.parse(document.getElementById('commission-editor-config').textContent);
  const el = id => document.getElementById(id);
  const rows = () => [...body.querySelectorAll('[data-assignment-row]')];
  const itemRows = id => rows().filter(r => r.dataset.ventaItemId === String(id));
  const vendor = r => r.querySelector('.assignment-vendedor').value;
  const quantity = r => r.querySelector('.assignment-qty');
  const name = id => cfg.vendors.find(v => String(v.id) === String(id))?.name || 'Sin vendedor';
  const money = cents => '$' + (cents / 100).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const integer = value => /^\d+$/.test(String(value)) && Number.isSafeInteger(Number(value)) ? Number(value) : null;
  const states = new Map();
  const priceStates = new Map();
  let sequence = 0, timer, processing = false, page = 1, direction = 1;
  const storageKey = `commission-view:${cfg.start}:${cfg.end}:${cfg.branch}`;
  let opened = new Set();
  try { opened = new Set(JSON.parse(sessionStorage.getItem(storageKey) || '[]')); } catch (_) {}
  const remember = () => { try {sessionStorage.setItem(storageKey, JSON.stringify([...opened]));} catch (_) {} };
  const initRow = row => {
    row.dataset.clientId ||= `r${row.dataset.tempId || 0}-${++sequence}`;
    const id = row.dataset.ventaItemId;
    if (!states.has(id)) states.set(id, {revision:row.dataset.revision, version:0, dirty:false, error:'', conflict:false});
  };
  rows().forEach(initRow);
  body.querySelectorAll('.assignment-group-row').forEach(r => r.remove());
  const status = () => {
    const all = [...states.values(), ...priceStates.values()];
    const dirty = all.filter(s => s.dirty).length;
    const errors = all.filter(s => s.error).length;
    const text = processing ? 'Guardando…' : errors ? `${errors} cambio(s) sin guardar: revisa el aviso` : dirty ? 'Cambios pendientes de guardar' : 'Todo guardado';
    el('commission-save-status').textContent = text;
    el('commission-save-status').className = errors ? 'text-danger' : dirty || processing ? 'text-warning' : 'text-success';
    el('commission-prices-status').textContent = [...priceStates.values()].find(s => s.error)?.error || text;
    el('commission-save-errors').innerHTML = [...states].filter(([,s]) => s.error).map(([id,s]) =>
      `<div class="alert alert-warning py-2">Artículo de venta ${esc(id)}: ${esc(s.error)} ${s.conflict ? `<button type="button" class="btn btn-sm btn-outline-secondary" data-reload-item="${esc(id)}">Revisar versión guardada</button>` : ''}</div>`).join('');
  };
  const balance = id => {
    const group = itemRows(id), primary = group.find(r => r.dataset.isPrimary === '1');
    if (!primary) return 'Falta la fila principal del reparto. Revisa la versión guardada.';
    const sold = integer(primary.dataset.totalItem);
    let others = 0;
    for (const row of group) {
      if (!vendor(row)) return 'Selecciona un vendedor válido.';
      if (row === primary) continue;
      const q = integer(quantity(row).value);
      if (q === null) return 'La cantidad debe ser un entero no negativo, sin decimales.';
      others += q;
    }
    if (sold === null || others > sold) return 'El reparto supera los bultos vendidos. Corrige las cantidades.';
    quantity(primary).value = String(sold - others);
    return '';
  };
  const centsFor = row => {
    const q = integer(quantity(row).value) ?? 0;
    const basis = Number(row.dataset.commissionBasisUnit || 1);
    const billable = Math.round((q * basis + Number.EPSILON) * 100) / 100;
    const cents = Math.round((billable * Number(row.dataset.comisionUnit || 0) + Number.EPSILON) * 100);
    return {q, commission:cents, sales:Math.round(q * Number(row.dataset.precioUsdUnit || 0) * 100)};
  };
  const totals = () => {
    const vendors = new Map(cfg.vendors.map(v => [String(v.id), {name:v.name, sold:0, assigned:0, sales:0, commission:0, pending:false}]));
    const bucket = id => {
      if (!vendors.has(id)) vendors.set(id,{name:name(id),sold:0,assigned:0,sales:0,commission:0,pending:false});
      return vendors.get(id);
    };
    const seen = new Set(), invoices = new Set();
    let sold = 0, qty = 0, sales = 0, commission = 0, pending = false;
    for (const row of rows()) {
      const values = centsFor(row), id = row.dataset.ventaItemId;
      const b = bucket(vendor(row));
      b.assigned += values.q; b.sales += values.sales; b.commission += values.commission;
      b.pending ||= row.dataset.commissionPending === '1'; pending ||= b.pending;
      qty += values.q; sales += values.sales; commission += values.commission;
      if (!seen.has(id)) {
        seen.add(id); const amount = integer(row.dataset.totalItem) || 0;
        sold += amount; bucket(row.dataset.vendedorOrigenId).sold += amount;
      }
      invoices.add(row.dataset.facturaId);
      row.querySelector('.assignment-commission-total').textContent = row.dataset.commissionPending === '1' ? 'Pendiente' : money(values.commission);
      row.classList.toggle('commission-dirty', !!states.get(id)?.dirty);
      row.classList.toggle('commission-invalid', !!states.get(id)?.error);
    }
    el('assign-total-vendidos').textContent = sold;
    el('assign-total-bultos').textContent = qty;
    el('assign-total-diff').textContent = qty - sold;
    el('assign-total-diff').className = qty !== sold ? 'text-danger fw-bold' : 'text-success fw-bold';
    el('assign-total-rows').textContent = rows().length;
    el('assign-total-facturas').textContent = invoices.size;
    el('assign-total-comision').textContent = money(commission) + (pending ? ' · parcial' : '');
    if (el('commission-pending-notice')) el('commission-pending-notice').hidden = !pending;
    el('assign-vendor-summary-body').innerHTML = [...vendors].sort((a,b) => a[1].name.localeCompare(b[1].name)).map(([id,v]) =>
      `<tr><td><button type="button" class="btn btn-link btn-sm p-0" data-show-vendor="${esc(id)}">${esc(v.name)}</button></td><td class="text-end">${v.sold}</td><td class="text-end">${v.assigned}</td><td class="text-end fw-bold commission-earned">${money(v.commission)}${v.pending ? ' · parcial' : ''}</td></tr>`).join('');
    el('assign-vendor-summary-total').textContent = money(commission) + (pending ? ' · parcial' : '');
    for (const button of body.querySelectorAll('[data-toggle-vendor]')) {
      const v = vendors.get(button.dataset.toggleVendor);
      if (v) button.querySelector('.group-totals').textContent = `Comisión ganada: ${money(v.commission)}${v.pending ? ' · parcial' : ''}`;
    }
    status();
  };
  const preserve = (action, anchor = document.activeElement?.closest('[data-assignment-row]')) => {
    const active = document.activeElement;
    const wrap = body.closest('.commission-grid-wrap');
    const y = window.scrollY, x = window.scrollX, left = wrap.scrollLeft, top = wrap.scrollTop;
    const before = anchor?.getBoundingClientRect().top;
    action();
    if (active?.isConnected) active.focus({preventScroll:true});
    wrap.scrollLeft = left; wrap.scrollTop = top;
    window.scrollTo(x, y);
    if (anchor?.isConnected && anchor.style.display !== 'none' && before != null) wrap.scrollTop += anchor.getBoundingClientRect().top - before;
  };
  const render = () => preserve(() => {
    body.querySelectorAll('.assignment-group-row').forEach(r => r.remove());
    const all = rows(), search = el('assignment-grid-search').value.trim().toLowerCase();
    const selected = el('commission-vendor-view').value;
    const grouped = new Map();
    all.forEach(row => {
      row.style.display = 'none';
      if (selected && vendor(row) !== selected) return;
      const saleFilter = el('commission-sale-filter')?.value;
      if (saleFilter === 'normal' && row.dataset.promotion === '1') return;
      if (saleFilter === 'promotion' && row.dataset.promotion !== '1') return;
      if (saleFilter === 'pending' && row.dataset.commissionPending !== '1') return;
      if (cfg.originVendor && row.dataset.vendedorOrigenId !== String(cfg.originVendor)) return;
      if (search && !(row.textContent + name(vendor(row))).toLowerCase().includes(search)) return;
      if (!grouped.has(vendor(row))) grouped.set(vendor(row), []);
      grouped.get(vendor(row)).push(row);
    });
    const groups = [...grouped].sort((a,b) => name(a[0]).localeCompare(name(b[0])));
    const size = Number(el('assignment-grid-size').value), pages = Math.max(1,Math.ceil(groups.length / size));
    page = Math.max(1,Math.min(page,pages));
    const field = el('assignment-grid-sort').value;
    const sortValue = r => {
      const indices = {factura:0,cliente:1,descripcion:2,precio:5,vendedor_facturacion:8};
      if (field === 'precio') return Number(r.dataset.precioUsdUnit);
      if (field === 'comision_unit') return Number(r.dataset.comisionUnit);
      if (field === 'comision_total') return centsFor(r).commission;
      if (field === 'cantidad') return integer(quantity(r).value) || 0;
      if (field === 'cant_vendida') return Number(r.dataset.totalItem);
      if (field === 'vendedor_asignado') return name(vendor(r));
      return r.cells[indices[field] ?? 0].textContent.trim();
    };
    for (const [id, group] of groups.slice((page-1)*size,page*size)) {
      const header = document.createElement('tr'); header.className = 'assignment-group-row';
      header.innerHTML = `<td colspan="11"><button type="button" class="commission-group-toggle" data-toggle-vendor="${esc(id)}" aria-expanded="${opened.has(id)}"><span>${opened.has(id) ? '▾' : '▸'} ${esc(name(id))} · ${group.length} filas</span><span class="group-totals"></span></button></td>`;
      body.append(header);
      group.sort((a,b) => typeof sortValue(a) === 'number' ? direction * (sortValue(a)-sortValue(b)) : direction * String(sortValue(a)).localeCompare(String(sortValue(b)),undefined,{numeric:true}));
      group.forEach(row => {body.append(row);row.style.display = opened.has(id) ? '' : 'none';});
    }
    el('assignment-grid-page').textContent = `${page} / ${pages}`;
    el('assignment-grid-prev').disabled = page <= 1;
    el('assignment-grid-next').disabled = page >= pages;
    totals();remember();
    el('commission-grid-count').textContent = `${[...grouped.values()].reduce((n,g)=>n+g.length,0)} de ${all.length} filas · ${groups.length} vendedores · Página ${page} de ${pages}`;
    el('commission-filter-empty').hidden = groups.length > 0 || all.length === 0;
    body.dispatchEvent(new CustomEvent('commission:grid-rendered', {bubbles:true}));
  });
  const schedule = () => {clearTimeout(timer);timer = setTimeout(drain,650);};
  const touch = row => {
    const id = row.dataset.ventaItemId, s = states.get(id);
    s.version++;s.dirty = true;
    if (!s.conflict) s.error = balance(id);
    totals();schedule();
  };
  async function post(url, data, isForm = false) {
    const controller = new AbortController(), timeout = setTimeout(() => controller.abort(),20000);
    try {
      const response = await fetch(url,{method:'POST',headers:{'X-Requested-With':'fetch','Accept':'application/json',...(isForm ? {} : {'Content-Type':'application/json'})},body:isForm ? data : JSON.stringify(data),signal:controller.signal});
      const payload = await response.json().catch(() => null);
      if (!response.ok || !payload?.ok) {
        const error = new Error(payload?.message || 'No se pudo confirmar el guardado. Reintenta; si la sesión venció, inicia sesión en otra pestaña.');
        error.conflict = response.status === 409;throw error;
      }
      return payload;
    } finally {clearTimeout(timeout);}
  }
  async function saveGroup(id,s) {
    s.error = balance(id);if (s.error) return;
    const version = s.version, group = itemRows(id);
    const payload = {item_id:Number(id),revision:s.revision,rows:group.map(row => ({temp_id:Number(row.dataset.tempId),client_id:row.dataset.clientId,vendedor_id:Number(vendor(row)),cantidad:quantity(row).value}))};
    try {
      const data = await post('/sales/comisiones/asignaciones/autoguardar',payload);
      s.revision = data.revision;
      for (const saved of data.rows) {
        const row = itemRows(id).find(r => r.dataset.clientId === saved.client_id);
        if (!row) continue;
        row.dataset.tempId = saved.temp_id;row.dataset.revision = data.revision;
        row.dataset.comisionUnit = data.comision_unit_usd ?? '0';row.dataset.commissionPending = data.comision_unit_usd == null ? '1' : '0';
        row.querySelector('.assignment-commission-unit').textContent = data.comision_unit_usd == null ? 'Sin configurar' : money(Math.round(Number(data.comision_unit_usd)*100));
      }
      if (version === s.version) s.dirty = false;
      s.error = '';s.conflict = false;
    } catch (error) {s.error = error.name === 'AbortError' ? 'La conexión tardó demasiado. No se pudo confirmar el guardado.' : error.message;s.conflict = !!error.conflict;}
  }
  const updateRates = rates => {
    for (const row of rows()) {
      const rate = rates[row.dataset.productoId];if (!rate) continue;
      const key = row.dataset.promotion === '1' ? 'comision_promocion_usd' : 'comision_usd';
      if (!(key in rate)) continue;
      row.dataset.comisionUnit = rate[key] ?? '0';row.dataset.commissionPending = rate[key] == null ? '1' : '0';
      row.querySelector('.assignment-commission-unit').textContent = rate[key] == null ? 'Sin configurar' : money(Math.round(Number(rate[key])*100));
    }
  };
  async function savePrice(row,s) {
    const version = s.version, data = new FormData();
    for (const input of row.querySelectorAll('input[name^="comision_"]')) {
      if (!input.checkValidity()) {s.error = 'Corrige el importe: usa un número no negativo con dos decimales.';return;}
      data.append(input.name,input.value);
    }
    try {
      const result = await post('/sales/comisiones/precios',data,true);
      if (version === s.version) s.dirty = false;
      s.error = '';updateRates(result.rates);
    } catch(error) {s.error = error.message;}
    row.classList.toggle('table-warning',s.dirty);row.title = s.error;
  }
  async function drain() {
    if (processing) return;
    processing = true;status();
    try {
      for (;;) {
        const price = [...priceStates].find(([,s]) => s.dirty && !s.error);
        if (price) {await savePrice(...price);totals();continue;}
        const group = [...states].find(([,s]) => s.dirty && !s.error);
        if (!group) break;
        await saveGroup(...group);totals();
      }
    } finally {processing = false;totals();}
  }
  body.addEventListener('input',event => {
    const row = event.target.closest('[data-assignment-row]');
    if (row && event.target.matches('.assignment-qty')) touch(row);
  });
  body.addEventListener('change',event => {
    const row = event.target.closest('[data-assignment-row]');
    if (row && event.target.matches('.assignment-vendedor')) {
      opened.add(vendor(row));touch(row);render();
    }
  });
  body.addEventListener('click',event => {
    const toggle = event.target.closest('[data-toggle-vendor]');
    if (toggle) {const id=toggle.dataset.toggleVendor;opened.has(id) ? opened.delete(id) : opened.add(id);render();return;}
    const row = event.target.closest('[data-assignment-row]');if (!row) return;
    if (event.target.closest('.assignment-split')) {
      const clone = row.cloneNode(true);clone.dataset.tempId='0';clone.dataset.clientId=`new-${++sequence}`;clone.dataset.isPrimary='0';
      quantity(clone).readOnly=false;quantity(clone).value='0';row.after(clone);initRow(clone);touch(clone);render();quantity(clone).focus({preventScroll:true});
    }
    if (event.target.closest('.assignment-remove')) {
      if (row.dataset.isPrimary === '1') return;
      const group = itemRows(row.dataset.ventaItemId);row.remove();touch(group.find(r => r !== row));render();
    }
  });
  body.addEventListener('keydown',event => {
    if (event.key !== 'Enter' || !event.target.matches('input,select')) return;
    event.preventDefault();const controls=[...body.querySelectorAll('.assignment-qty,.assignment-vendedor')].filter(i => i.closest('tr').style.display !== 'none');
    controls[controls.indexOf(event.target)+(event.shiftKey ? -1 : 1)]?.focus({preventScroll:true});
  });
  el('commission-vendor-view').addEventListener('change',()=>{const v=el('commission-vendor-view').value;if(v)opened.add(v);page=1;render();});
  el('assign-vendor-summary-body').addEventListener('click',event=>{
    const button=event.target.closest('[data-show-vendor]');if(!button)return;
    el('commission-vendor-view').value=button.dataset.showVendor;opened.add(button.dataset.showVendor);page=1;render();
  });
  el('commission-expand-all').addEventListener('click',()=>{rows().forEach(r=>opened.add(vendor(r)));render();});
  el('commission-collapse-all').addEventListener('click',()=>{opened.clear();render();});
  for (const id of ['assignment-grid-search','assignment-grid-sort','assignment-grid-size']) el(id).addEventListener(id.endsWith('search')?'input':'change',()=>{page=1;render();});
  document.addEventListener('commission:grid-refresh',()=>{page=1;render();});
  el('assignment-grid-dir').addEventListener('click',()=>{direction*=-1;el('assignment-grid-dir').dataset.dir=direction===1?'asc':'desc';el('assignment-grid-dir').textContent=direction===1?'Ascendente':'Descendente';render();});
  el('assignment-grid-prev').addEventListener('click',()=>{page--;render();});
  el('assignment-grid-next').addEventListener('click',()=>{page++;render();});
  const retry = () => {for(const [id,s] of states)if(s.dirty&&!s.conflict)s.error=balance(id);for(const s of priceStates.values())s.error='';drain();};
  el('assignment-retry').addEventListener('click',retry);
  const pricesForm=document.querySelector('form[action="/sales/comisiones/precios"]');
  pricesForm.addEventListener('input',event=>{
    const row=event.target.closest('[data-product-row]');if(!row)return;
    const s=priceStates.get(row)||{version:0,dirty:false,error:''};s.version++;s.dirty=true;s.error='';priceStates.set(row,s);row.classList.add('table-warning');status();schedule();
  });
  pricesForm.addEventListener('submit',event=>{event.preventDefault();retry();});
  const pending = () => processing || [...states.values(),...priceStates.values()].some(s=>s.dirty);
  form.addEventListener('submit',event=>{
    if (!event.submitter?.getAttribute('formaction')) {event.preventDefault();retry();return;}
    if (pending()) {event.preventDefault();el('commission-save-status').textContent='Espera el guardado o corrige los cambios pendientes antes de ejecutar esta acción.';return;}
    if (!window.confirm('Esta acción modifica el cierre o las asignaciones del periodo. ¿Deseas continuar?'))event.preventDefault();
  });
  window.addEventListener('beforeunload',event=>{if(pending()){event.preventDefault();event.returnValue='';}});
  window.addEventListener('online',retry);
  el('commission-save-errors').addEventListener('click',async event=>{
    const button=event.target.closest('[data-reload-item]');if(!button)return;
    const id=button.dataset.reloadItem;
    if(!window.confirm('Se mostrará el reparto guardado para este artículo. Los cambios locales de este artículo se descartarán. ¿Continuar?'))return;
    try {
      const response=await fetch(`/sales/comisiones/asignaciones/estado/${id}`,{headers:{Accept:'application/json'}});
      const data=await response.json();if(!response.ok||!data.ok)throw new Error(data.message||'No se pudo consultar el reparto.');
      preserve(()=>{
        const original=itemRows(id),template=original[0].cloneNode(true);original.forEach(r=>r.remove());
        data.rows.forEach((saved,index)=>{
          const r=template.cloneNode(true);r.dataset.totalItem=data.sold_quantity;r.dataset.tempId=saved.temp_id;r.dataset.clientId=`loaded-${++sequence}`;r.dataset.revision=data.revision;r.dataset.isPrimary=index===0?'1':'0';
          quantity(r).value=saved.cantidad;quantity(r).readOnly=index===0;r.querySelector('.assignment-vendedor').value=saved.vendedor_id;
          r.dataset.comisionUnit=data.comision_unit_usd??'0';r.dataset.commissionPending=data.comision_unit_usd==null?'1':'0';
          r.querySelector('.assignment-commission-unit').textContent=data.comision_unit_usd==null?'Sin configurar':money(Math.round(Number(data.comision_unit_usd)*100));
          body.append(r);
        });
        states.set(id,{revision:data.revision,version:0,dirty:false,error:'',conflict:false});render();
      });
    }catch(error){states.get(id).error=error.message;status();}
  });
  let reportRequest = 0;
  async function refreshReports(reportForm) {
    const serial = ++reportRequest;
    const current = el('tab-reportes');
    const query = reportForm ? new URLSearchParams(new FormData(reportForm)) : new URLSearchParams(window.location.search);
    if (!reportForm) {
      const existing = current.querySelector('form');
      if (existing) for (const [key,value] of new FormData(existing)) query.set(key,value);
    }
    let notice = current.querySelector('[data-report-status]');
    if (!notice) {notice = document.createElement('div');notice.dataset.reportStatus='1';notice.className='alert alert-light';current.prepend(notice);}
    notice.textContent='Actualizando reportes guardados…';
    try {
      const response=await fetch('/sales/comisiones/reportes/vista?'+query.toString(),{headers:{'X-Requested-With':'fetch'}});
      const html=await response.text();
      const next=new DOMParser().parseFromString(html,'text/html').getElementById('tab-reportes');
      if (!response.ok || !next)throw new Error('No se pudo actualizar el reporte. Reintenta seleccionando Reportes comisiones.');
      if(serial!==reportRequest)return;
      next.classList.toggle('d-none',current.classList.contains('d-none'));
      preserve(()=>current.replaceWith(next),null);
    }catch(error){notice.textContent=error.message;notice.className='alert alert-warning';}
  }
  document.addEventListener('click',event=>{
    if(event.target.closest('[data-tab-target="reportes"]')) {
      if(pending()){el('tab-reportes').insertAdjacentHTML('afterbegin','<div class="alert alert-warning">Hay cambios pendientes. El reporte muestra solo lo guardado.</div>');}
      refreshReports();
    }
  });
  document.addEventListener('submit',event=>{
    const reportForm=event.target.closest('#tab-reportes form');
    if(reportForm){event.preventDefault();refreshReports(reportForm);}
  });
  el('assignment-grid-search').value = cfg.productQuery || '';
  if(el('commission-vendor-view').value)opened.add(el('commission-vendor-view').value);
  render();
})();
