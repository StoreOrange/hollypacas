(() => {
  const form = document.querySelector('#rq-form');
  const openLink = document.querySelector('[data-new-request]');
  openLink?.addEventListener('click', () => { document.querySelector('#rq-new').open = true; });
  document.querySelectorAll('[data-decision]').forEach(decision => decision.addEventListener('submit', () => {
    decision.querySelectorAll('button[type="submit"]').forEach(button => { button.disabled = true; button.textContent = 'Procesando…'; });
  }));
  const share = document.querySelector('[data-share-receipt]');
  share?.addEventListener('click', async () => {
    const status = document.querySelector('#rq-share-status');
    share.disabled = true;
    try {
      const response = await fetch(share.dataset.shareReceipt, { credentials: 'same-origin' });
      if (!response.ok || !response.headers.get('content-type')?.startsWith('image/jpeg')) throw new Error('No se pudo cargar el comprobante. Recarga la página.');
      const blob = await response.blob();
      const file = new File([blob], share.dataset.filename, { type: 'image/jpeg' });
      if (navigator.canShare?.({ files: [file] })) {
        await navigator.share({ files: [file], title: 'Comprobante de traslado · Pacas Holl' });
        status.textContent = 'Comprobante listo para compartir.';
      } else {
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.href = url; link.download = share.dataset.filename; link.click();
        setTimeout(() => URL.revokeObjectURL(url), 30000);
        status.textContent = 'JPG descargado. Puedes adjuntarlo en WhatsApp u otra aplicación.';
      }
    } catch (error) { if (error.name !== 'AbortError') status.textContent = error.message; }
    finally { share.disabled = false; }
  });
  if (!form) return;
  const query = document.querySelector('#rq-query');
  const results = document.querySelector('#rq-results');
  const searchStatus = document.querySelector('#rq-search-status');
  const cart = document.querySelector('#rq-cart');
  const hidden = document.querySelector('#rq-items');
  const error = document.querySelector('#rq-form-error');
  const selected = new Map();
  let timer, controller, serial = 0, focused = -1;
  const node = (tag, text, className) => {
    const element = document.createElement(tag);
    if (text !== undefined) element.textContent = text;
    if (className) element.className = className;
    return element;
  };
  function sync() {
    hidden.value = JSON.stringify([...selected.values()].map(item => ({ id: item.id, quantity: item.quantity })));
  }
  function renderCart() {
    cart.replaceChildren();
    if (!selected.size) cart.append(node('p', 'Agrega los productos que necesitas.', 'rq-empty'));
    selected.forEach(item => {
      const row = node('div', undefined, 'rq-cart-item');
      const description = node('div');
      description.append(node('strong', item.code), node('p', item.description), node('p', `${results.dataset.local}: ${item.local} · ${results.dataset.source}: ${item.source}`));
      const control = node('div');
      const label = node('label', 'Unidades enteras');
      label.htmlFor = `rq-qty-${item.id}`;
      const input = node('input', undefined, 'form-control');
      Object.assign(input, { id: label.htmlFor, type: 'number', min: '1', max: '1000000', step: '1', required: true, value: item.quantity });
      input.inputMode = 'numeric';
      input.addEventListener('input', () => { item.quantity = input.value; sync(); });
      control.append(label, input);
      const remove = node('button', '×', 'rq-remove');
      remove.type = 'button'; remove.setAttribute('aria-label', `Quitar ${item.code}`);
      remove.addEventListener('click', () => { selected.delete(item.id); renderCart(); });
      row.append(description, control, remove); cart.append(row);
    });
    sync();
  }
  function choose(item) {
    if (selected.has(item.id)) {
      document.querySelector(`#rq-qty-${item.id}`)?.focus();
      searchStatus.textContent = `${item.code} ya está en tu solicitud. Ajusta su cantidad.`;
      return;
    }
    if (selected.size >= 60) { searchStatus.textContent = 'Puedes agregar hasta 60 productos por solicitud.'; return; }
    selected.set(item.id, { ...item, quantity: '1' });
    renderCart();
    results.replaceChildren(); query.value = ''; query.setAttribute('aria-expanded', 'false'); query.focus();
    searchStatus.textContent = `${item.code} agregado. Puedes buscar otro producto.`;
  }
  query.addEventListener('input', () => {
    clearTimeout(timer); controller?.abort(); serial += 1; focused = -1;
    const version = serial;
    if (query.value.trim().length < 2) { results.replaceChildren(); query.setAttribute('aria-expanded', 'false'); searchStatus.textContent = 'Escribe al menos 2 letras.'; return; }
    timer = setTimeout(async () => {
      controller = new AbortController(); searchStatus.textContent = 'Consultando existencias actuales…';
      try {
        const params = new URLSearchParams({ q: query.value.trim(), destination: form.dataset.destination });
        const response = await fetch(`/sales/requisas/productos?${params}`, { signal: controller.signal, credentials: 'same-origin', headers: { Accept: 'application/json' }, cache: 'no-store' });
        if (!response.ok || !response.headers.get('content-type')?.includes('application/json')) throw new Error('No se pudo consultar. Verifica tu sesión e inténtalo de nuevo.');
        const data = await response.json();
        if (version !== serial) return;
        results.replaceChildren();
        data.items.forEach((item, index) => {
          const button = node('button', undefined, 'rq-search-result');
          button.type = 'button'; button.id = `rq-result-${index}`; button.setAttribute('role', 'option'); button.setAttribute('aria-selected', 'false');
          button.append(node('strong', item.code), node('small', item.description), node('span', `${results.dataset.local}: ${item.local} · ${results.dataset.source}: ${item.source}`, item.source > 0 ? 'text-success' : 'text-danger'));
          button.addEventListener('click', () => choose(item)); results.append(button);
        });
        query.setAttribute('aria-expanded', String(data.items.length > 0));
        searchStatus.textContent = data.items.length ? 'Selecciona un producto para agregarlo. Los saldos se verifican nuevamente al autorizar.' : 'No encontramos productos con esa búsqueda.';
      } catch (e) { if (e.name !== 'AbortError' && version === serial) { results.replaceChildren(); searchStatus.textContent = e.message; } }
    }, 250);
  });
  query.addEventListener('keydown', event => {
    const options = [...results.querySelectorAll('button')];
    if (event.key === 'Escape') { results.replaceChildren(); query.setAttribute('aria-expanded', 'false'); return; }
    if (event.key === 'Enter') { event.preventDefault(); if (focused >= 0) options[focused]?.click(); return; }
    if (!['ArrowDown', 'ArrowUp'].includes(event.key) || !options.length) return;
    event.preventDefault(); focused = (focused + (event.key === 'ArrowDown' ? 1 : -1) + options.length) % options.length;
    options.forEach((option, index) => option.setAttribute('aria-selected', String(index === focused)));
    query.setAttribute('aria-activedescendant', options[focused].id); options[focused].scrollIntoView({ block: 'nearest' });
  });
  const warehouse = document.querySelector('select#rq-destination');
  warehouse?.addEventListener('change', () => {
    if (selected.size && !window.confirm('Cambiar la bodega vaciará los productos de esta solicitud. ¿Continuar?')) { warehouse.value = form.dataset.destination; return; }
    window.location.assign(`/sales/requisas?destination=${encodeURIComponent(warehouse.value)}&new=1`);
  });
  form.addEventListener('submit', event => {
    error.textContent = '';
    if (!selected.size || [...selected.values()].some(item => !/^[1-9][0-9]*$/.test(String(item.quantity)) || Number(item.quantity) > 1000000)) {
      event.preventDefault(); error.textContent = 'Agrega productos e indica cantidades enteras mayores que cero.'; return;
    }
    sync();
    const submit = document.querySelector('#rq-submit'); submit.disabled = true; submit.textContent = 'Enviando solicitud…';
  });
})();
