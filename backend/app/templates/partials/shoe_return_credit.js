  const creditModalEl = document.getElementById('return-credit-modal');
  const creditEscape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const isReturnCredit = () => (formaPagoField?.selectedOptions[0]?.textContent || '').trim().toLowerCase() === 'anticipo';
  let availableReturnCredits = [], creditSearchGeneration = 0;
  const loadReturnCredits = async () => {
    const status = document.getElementById('return-credit-status');
    const generation = ++creditSearchGeneration;
    const query = document.getElementById('return-credit-query').value.trim();
    const params = new URLSearchParams({q:query});
    const customerId = document.getElementById('cliente-id')?.value;
    if (!query && customerId) params.set('cliente_id', customerId);
    status.textContent = 'Buscando anticipos…';
    try {
      const response = await fetch('/sales/devoluciones/anticipos?' + params, {headers:{Accept:'application/json'}});
      const data = await response.json();
      if (!response.ok || !data.ok) throw new Error(data.message || data.detail || 'No se pudieron consultar los anticipos.');
      if (generation !== creditSearchGeneration) return;
      availableReturnCredits = data.items.filter(r => !paymentItems.some(p => p.devolucionId === r.id));
      status.textContent = availableReturnCredits.length ? 'Elige un anticipo. Se aplicará su valor completo y se seleccionará su cliente.' : 'No hay anticipos disponibles para esta búsqueda. Puedes buscar por número de factura o nombre.';
      document.getElementById('return-credit-list').innerHTML = availableReturnCredits.map((r,index) => `<div class="border rounded-3 p-3"><div class="d-flex justify-content-between flex-wrap gap-2"><div><strong>${creditEscape(r.numero)} · ${creditEscape(r.cliente)}</strong><div class="small text-muted">Factura ${creditEscape(r.factura)} · ${creditEscape(r.fecha)} · ${creditEscape(r.hora)}</div><div class="mt-2">${creditEscape(r.producto)} · ${creditEscape(r.variante)}</div><div class="small text-muted">${r.cantidad} unidad(es) · ${creditEscape(r.motivo)}</div></div><div class="text-end"><div class="fs-4 fw-bold text-primary">${formatMoney(r.monto_cs,'CS')}</div><button type="button" class="btn btn-primary mt-2" data-credit-index="${index}">Aplicar anticipo completo</button></div></div></div>`).join('');
    } catch(error) { status.textContent=error.message; }
  };
  const openReturnCredits = () => {
    if ((monedaField?.value || 'CS') !== 'CS' || (document.getElementById('condicion-venta')?.value || 'CONTADO') === 'CREDITO') {
      pauseNavForAlert({icon:'warning',title:'Canje en C$',text:'Los anticipos se aplican en compras de contado en C$.'});
      return;
    }
    const payModalEl = document.getElementById('payment-modal');
    const creditModal = bootstrap.Modal.getOrCreateInstance(creditModalEl);
    document.getElementById('return-credit-query').value='';
    if (payModalEl.classList.contains('show')) {
      payModalEl.addEventListener('hidden.bs.modal',()=>creditModal.show(),{once:true});
      bootstrap.Modal.getOrCreateInstance(payModalEl).hide();
    } else creditModal.show();
    loadReturnCredits();
  };
  creditModalEl.addEventListener('hidden.bs.modal', () => bootstrap.Modal.getOrCreateInstance(document.getElementById('payment-modal')).show());
  creditModalEl.addEventListener('shown.bs.modal', () => document.getElementById('return-credit-query').focus());
  document.getElementById('return-credit-search').addEventListener('click', loadReturnCredits);
  document.getElementById('return-credit-query').addEventListener('keydown', event => {if(event.key==='Enter'){event.preventDefault();loadReturnCredits();}});
  document.getElementById('return-credit-list').addEventListener('click', event => {
    const button=event.target.closest('[data-credit-index]');if(!button)return;
    const credit=availableReturnCredits[Number(button.dataset.creditIndex)];if(!credit)return;
    const status=document.getElementById('return-credit-status');
    if(paymentItems.some(p=>p.devolucionId===credit.id)){status.textContent='Ese anticipo ya está agregado.';return;}
    const previousCredit=paymentItems.find(p=>p.devolucionId);
    if(previousCredit && previousCredit.clienteId!==credit.cliente_id){status.textContent='Los anticipos de una factura deben pertenecer al mismo cliente.';return;}
    const paid=paymentItems.reduce((total,p)=>total+toCs(p.monto,p.moneda),0);
    if(Math.round((paid+credit.monto_cs)*100)>Math.round(currentTotals.cs*100)){status.textContent='La compra debe costar igual o más que el anticipo. Quita otros pagos o agrega el producto de mayor valor antes de aplicarlo.';return;}
    const formOption=Array.from(formaPagoField.options).find(o=>(o.textContent||'').trim().toLowerCase()==='anticipo');
    setClienteSelection(credit.cliente_id,credit.cliente);
    paymentItems.push({formaId:formOption.value,formaLabel:`Anticipo ${credit.numero}`,moneda:'CS',monto:credit.monto_cs,bancoId:'',cuentaId:'',bancoLabel:'Factura '+credit.factura,devolucionId:credit.id,clienteId:credit.cliente_id});
    renderPaymentList();setPaymentAmountDefault();
    bootstrap.Modal.getOrCreateInstance(creditModalEl).hide();
  });
