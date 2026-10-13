(() => {
  const el = id => document.getElementById('unit-' + id);
  let rows = [], saving = false;
  const token = JSON.parse(el('data').textContent).token;
  const fields = ['code', 'name', 'line', 'brand', 'color', 'size', 'qty', 'cost', 'price', 'scan'];
  const escape = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  function feedback(message, error = true) {
    const box = el('feedback'); box.className = 'alert ' + (error ? 'alert-danger' : 'alert-info'); box.textContent = message;
  }
  function render() {
    el('rows').innerHTML = rows.length ? rows.map((r, i) => `<tr><td><strong>${escape(r.code)}</strong><div>${escape(r.name)}</div></td><td>${escape(r.color)}</td><td>${escape(r.size)}</td><td>${r.qty}</td><td>${Number(r.cost).toFixed(2)}</td><td>${Number(r.price).toFixed(2)}</td><td>${escape(r.scan_code || 'Automático')}</td><td><button type="button" class="btn btn-outline-danger btn-sm" data-remove="${i}" aria-label="Quitar artículo"><i class="bi bi-trash"></i></button></td></tr>`).join('') : '<tr><td colspan="8" class="text-muted text-center">Agrega el primer artículo.</td></tr>';
    el('summary').textContent = `${rows.length} filas · ${rows.reduce((sum, r) => sum + r.qty, 0)} unidades · Costo total C$ ${rows.reduce((sum, r) => sum + Math.round(Number(r.cost) * 100) * r.qty, 0) / 100}`;
    el('save').disabled = !rows.length || saving;
  }
  let searchTimer, searchAbort, searchSequence = 0, found = [], selected = -1;
  function closeSearch() {
    el('search-results').classList.add('d-none'); el('code').setAttribute('aria-expanded', 'false');
    el('code').removeAttribute('aria-activedescendant');
  }
  function highlight(index) {
    if (!found.length) return;
    selected = (index + found.length) % found.length;
    el('search-results').querySelectorAll('[data-pick]').forEach((button, i) => {
      button.classList.toggle('active', i === selected); button.setAttribute('aria-selected', String(i === selected));
      if (i === selected) { el('code').setAttribute('aria-activedescendant', button.id); button.scrollIntoView({block:'nearest'}); }
    });
  }
  function pick(index) {
    const data = found[index]; if (!data) return;
    clearTimeout(searchTimer); searchAbort?.abort(); searchSequence++;
    const mapping = {code:'code',name:'name',line:'line_id',brand:'brand',cost:'cost',price:'price',scan:'scan_code',size:'size',color:'color'};
    Object.entries(mapping).forEach(([id,key]) => {el(id).value = data[key] ?? '';});
    closeSearch(); found = [];
    el('lookup-status').textContent = data.kind === 'variant' ? 'Variante seleccionada: color y talla cargados.' : 'Modelo seleccionado: elige color y talla para este ingreso.';
    el(data.kind === 'variant' ? 'qty' : 'color').focus();
  }
  async function runSearch() {
    clearTimeout(searchTimer); searchAbort?.abort(); const sequence = ++searchSequence;
    const query = el('code').value.trim(); found = []; selected = -1; closeSearch();
    if (query.length < 2) { el('lookup-status').textContent = 'Escribe al menos 2 caracteres.'; return; }
    searchAbort = new AbortController(); el('lookup-status').textContent = 'Buscando…';
    try {
      const response = await fetch('/inventory/ingresos-unidad/search?q=' + encodeURIComponent(query), {signal:searchAbort.signal});
      if (!response.ok) throw new Error('No se pudo consultar el catálogo');
      const data = await response.json(); if (sequence !== searchSequence) return;
      found = data.items || [];
      if (!found.length) { el('lookup-status').textContent = 'Sin coincidencias. Puedes completar los datos para crear un artículo nuevo.'; return; }
      el('search-results').innerHTML = found.map((r, i) => `<button id="unit-match-${i}" type="button" class="unit-search-result" role="option" data-pick="${i}"><strong>${escape(r.scan_code || r.code)} · ${escape(r.name)}</strong><small>${r.kind === 'variant' ? 'Variante · ' + escape(r.color) + ' · Talla ' + escape(r.size) : 'Modelo · ' + escape(r.brand || 'Sin Marca')} · C$ ${Number(r.price).toFixed(2)}</small></button>`).join('');
      el('search-results').classList.remove('d-none'); el('code').setAttribute('aria-expanded', 'true'); highlight(0);
      el('lookup-status').textContent = data.limited ? 'Mostrando hasta 12 coincidencias. Afina la búsqueda.' : `${found.length} coincidencias. Selecciona con clic o flechas y Enter.`;
    } catch (error) { if (error.name !== 'AbortError' && sequence === searchSequence) { closeSearch(); el('lookup-status').textContent = error.message; } }
  }
  el('lookup').addEventListener('click', runSearch);
  el('code').addEventListener('input', () => {
    clearTimeout(searchTimer); searchAbort?.abort(); searchSequence++; found = []; selected = -1; closeSearch();
    searchTimer = setTimeout(runSearch, 220);
  });
  el('code').addEventListener('keydown', event => {
    if (event.key === 'Escape') { closeSearch(); return; }
    if (['ArrowDown','ArrowUp'].includes(event.key) && found.length && !el('search-results').classList.contains('d-none')) {
      event.preventDefault(); highlight(selected + (event.key === 'ArrowDown' ? 1 : -1));
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      if (found.length && !el('search-results').classList.contains('d-none')) pick(selected); else runSearch();
    }
  });
  el('search-results').addEventListener('click', event => { const button = event.target.closest('[data-pick]'); if (button) pick(Number(button.dataset.pick)); });
  document.addEventListener('click', event => { if (!event.target.closest('.unit-search-wrap')) closeSearch(); });
  el('add').addEventListener('click', () => {
    const r = {code:el('code').value.trim().toUpperCase(),name:el('name').value.trim(),line_id:el('line').value,brand:el('brand').value.trim(),color:el('color').value.trim(),size:el('size').value.trim(),qty:Number(el('qty').value),cost:el('cost').value,price:el('price').value,scan_code:el('scan').value.trim().toUpperCase()};
    if (!r.code || !r.name || !r.line_id || !r.color || !r.size) return feedback('Completa código, descripción, línea, color y talla.');
    if (!Number.isInteger(r.qty) || r.qty < 1 || r.qty > 10000) return feedback('La cantidad debe ser un entero entre 1 y 10,000.');
    if (!el('cost').checkValidity() || !el('price').checkValidity() || !r.price || Number(r.price) <= Number(r.cost)) return feedback('Revisa costo y precio: máximo dos decimales y precio mayor que el costo.');
    if (rows.some(x => x.code === r.code && x.color.toLowerCase() === r.color.toLowerCase() && x.size === r.size)) return feedback('Esta talla/color ya está en la lista. Quita la fila y agrégala con la cantidad correcta.');
    if (rows.some(x => x.code === r.code && (Number(x.cost) !== Number(r.cost) || Number(x.price) !== Number(r.price)))) return feedback('El mismo modelo debe tener el mismo costo y precio.');
    if (rows.length >= 100) return feedback('El límite es 100 filas por ingreso.');
    rows.push(r); render(); el('feedback').className = ''; el('feedback').textContent = '';
    el('size').value = ''; el('scan').value = ''; el('qty').value = '1'; el('size').focus();
  });
  el('rows').addEventListener('click', event => { const button = event.target.closest('[data-remove]'); if (!button || saving) return; rows.splice(Number(button.dataset.remove), 1); render(); });
  fields.forEach((id, index) => el(id).addEventListener('keydown', event => {
    if (event.key !== 'Enter' || id === 'code') return; event.preventDefault();
    if (id === 'code') el('lookup').click(); else if (index === fields.length - 1) el('add').click(); else el(fields[index + 1]).focus();
  }));
  el('form').addEventListener('submit', async event => {
    event.preventDefault(); if (saving || !rows.length) return;
    saving = true; render(); el('save').textContent = 'Registrando…';
    try {
      const response = await fetch('/inventory/ingresos-unidad', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token,rows,date:el('date').value,warehouse_id:el('warehouse').value,type_id:el('type').value,supplier_id:el('supplier').value,note:el('note').value})});
      const data = await response.json(); if (!response.ok || !data.ok) throw new Error(data.error || 'No se pudo registrar el ingreso');
      el('form').classList.add('d-none'); el('feedback').className = ''; el('feedback').textContent = '';
      el('result').classList.remove('d-none'); el('result-text').textContent = `Ingreso #${data.receipt_id}. Los productos ya están disponibles en la bodega seleccionada.`;
      el('labels').href = data.labels_url;
    } catch (error) { feedback(error.message + '. La lista se conserva para corregir o reintentar.'); }
    finally { saving = false; render(); el('save').innerHTML = '<i class="bi bi-check-circle me-1"></i> Registrar ingreso'; }
  });
  el('new').addEventListener('click', () => location.reload());
  document.querySelectorAll('.toggle-sidebar').forEach(button => button.addEventListener('click', () => document.getElementById('layout').classList.toggle('sidebar-collapsed')));
  render();
})();
