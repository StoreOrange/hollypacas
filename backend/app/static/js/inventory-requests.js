(() => {
  document.body.classList.add('rq-workspace');
  const editor = document.querySelector('#rq-new');
  const setWorkspace = () => document.body.classList.toggle('rq-composing', !!editor?.open);
  editor?.addEventListener('toggle', setWorkspace); setWorkspace();
  const keyboard = () => document.body.classList.toggle('rq-keyboard-open', window.matchMedia('(max-width:767px)').matches && !!window.visualViewport && window.innerHeight - window.visualViewport.height > 130);
  window.visualViewport?.addEventListener('resize', keyboard);
  document.addEventListener('focusout', () => setTimeout(keyboard, 100));
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
  const preferences = document.querySelector('#rq-preferences');
  if (window.matchMedia('(max-width:767px)').matches) preferences.open = false;
  document.querySelector('#rq-seller').addEventListener('change', event => {
    document.querySelector('#rq-seller-summary').textContent = event.target.selectedOptions[0]?.textContent || 'Sin vendedor';
  });
  const query = document.querySelector('#rq-query');
  const results = document.querySelector('#rq-results');
  const searchStatus = document.querySelector('#rq-search-status');
  const cart = document.querySelector('#rq-cart');
  const hidden = document.querySelector('#rq-items');
  const error = document.querySelector('#rq-form-error');
  const selected = new Map();
  let timer, controller, serial = 0, focused = -1, confirmed = false, busy = false;
  const review = document.querySelector('#rq-review');
  const confirm = document.querySelector('#rq-confirm');
  const node = (tag, text, className) => {
    const element = document.createElement(tag);
    if (text !== undefined) element.textContent = text;
    if (className) element.className = className;
    return element;
  };
  function sync() {
    hidden.value = JSON.stringify([...selected.values()].map(item => ({ id: item.id, quantity: item.quantity })));
    const units = [...selected.values()].reduce((sum, item) => sum + (/^[1-9][0-9]*$/.test(String(item.quantity)) ? Number(item.quantity) : 0), 0);
    document.querySelector('#rq-cart-count').textContent = `${selected.size} ${selected.size === 1 ? 'producto' : 'productos'}`;
    document.querySelector('#rq-unit-count').textContent = `${units.toLocaleString('es-NI')} ${units === 1 ? 'unidad' : 'unidades'}`;
    document.querySelector('.rq-compose-total small').textContent = selected.size ? 'Revisa antes de solicitar' : 'Agrega productos para continuar';
    document.querySelector('#rq-submit').disabled = busy || !selected.size || !document.querySelector('#rq-seller').options.length;
  }
  function stockPills(item) {
    const pills = node('div', undefined, 'rq-stock-pills');
    [[results.dataset.local, item.local, false], [results.dataset.source, item.source, true]].forEach(([name, quantity, origin]) => {
      const pill = node('span', undefined, `rq-stock-pill${origin ? ' is-source' : ''}${origin && quantity <= 0 ? ' is-empty' : ''}`);
      pill.append(node('small', origin ? `${name} puede enviar` : `En ${name}`), node('b', quantity <= 0 ? '0 disponibles' : `${quantity} disponibles`));
      pills.append(pill);
    });
    return pills;
  }
  function renderCart() {
    cart.replaceChildren();
    if (!selected.size) {
      const empty = node('div', undefined, 'rq-cart-empty');
      const icon = node('i', undefined, 'bi bi-box-seam'); icon.setAttribute('aria-hidden', 'true');
      empty.append(icon, node('strong', '¿Qué pacas necesitas?'), node('p', 'Busca arriba y agrega los productos a tu solicitud.'));
      cart.append(empty);
    }
    selected.forEach(item => {
      const row = node('div', undefined, 'rq-cart-item');
      const description = node('div');
      description.append(node('strong', item.code), node('p', item.description), node('p', `${results.dataset.local}: ${item.local} · ${results.dataset.source}: ${item.source}`, 'rq-item-stock'));
      const control = node('div');
      const label = node('label', 'Cantidad a solicitar'); label.htmlFor = `rq-qty-${item.id}`;
      const stepper = node('div', undefined, 'rq-qty-control');
      const input = node('input', undefined, 'form-control');
      Object.assign(input, { id: label.htmlFor, type: 'number', min: '1', max: '1000000', step: '1', required: true, value: item.quantity });
      input.inputMode = 'numeric'; input.setAttribute('aria-label', `Cantidad de ${item.code}`);
      const minus = node('button', '−'); minus.type = 'button'; minus.setAttribute('aria-label', `Quitar una unidad de ${item.code}`);
      const plus = node('button', '+'); plus.type = 'button'; plus.setAttribute('aria-label', `Agregar una unidad de ${item.code}`);
      const warning = node('p', undefined, 'rq-stock-warning'); warning.hidden = true;
      function changed() {
        item.quantity = input.value; confirmed = false;
        minus.disabled = Number(input.value) <= 1; plus.disabled = Number(input.value) >= 1000000;
        warning.hidden = !(Number(input.value) > item.source);
        warning.textContent = `Solicitas más de las ${item.source} disponibles en ${results.dataset.source}. El saldo se revisará al autorizar.`;
        sync();
      }
      input.addEventListener('input', changed);
      minus.addEventListener('click', () => { input.value = String(Math.max(1, Math.trunc(Number(input.value) || 1) - 1)); changed(); });
      plus.addEventListener('click', () => { input.value = String(Math.min(1000000, Math.max(1, Math.trunc(Number(input.value) || 0) + 1))); changed(); });
      stepper.append(minus, input, plus); control.append(label, stepper);
      const remove = node('button', '×', 'rq-remove'); remove.type = 'button'; remove.setAttribute('aria-label', `Quitar ${item.code}`);
      remove.addEventListener('click', () => { selected.delete(item.id); confirmed = false; renderCart(); });
      row.append(description, control, remove, warning); cart.append(row); changed();
    });
    sync();
  }
  function choose(item) {
    clearTimeout(timer); controller?.abort(); serial += 1; focused = -1;
    if (selected.has(item.id)) {
      document.querySelector(`#rq-qty-${item.id}`)?.focus();
      searchStatus.textContent = `${item.code} ya está en tu solicitud. Ajusta su cantidad.`;
      return;
    }
    if (selected.size >= 60) { searchStatus.textContent = 'Puedes agregar hasta 60 productos por solicitud.'; return; }
    selected.set(item.id, { ...item, quantity: '1' });
    renderCart();
    results.replaceChildren(); query.value = ''; query.setAttribute('aria-expanded', 'false'); query.removeAttribute('aria-activedescendant');
    document.querySelector(`#rq-qty-${item.id}`)?.focus();
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
          button.append(node('strong', item.code), node('small', item.description), stockPills(item));
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
    if (event.key === 'Enter') { event.preventDefault(); if (options.length) options[Math.max(0, focused)]?.click(); return; }
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
  document.querySelectorAll('[data-review-close]').forEach(button => button.addEventListener('click', () => review.close()));
  review.addEventListener('close', () => { if (!busy) confirmed = false; });
  confirm.addEventListener('click', () => {
    if (busy) return;
    confirmed = true; review.close(); form.requestSubmit();
  });
  form.addEventListener('submit', event => {
    error.textContent = '';
    if (busy) { event.preventDefault(); return; }
    if (!selected.size || [...selected.values()].some(item => !/^[1-9][0-9]*$/.test(String(item.quantity)) || Number(item.quantity) > 1000000)) {
      event.preventDefault(); error.textContent = 'Agrega productos e indica cantidades enteras mayores que cero.';
      error.scrollIntoView({ block: 'center', behavior: 'smooth' }); return;
    }
    sync();
    if (!confirmed) {
      event.preventDefault();
      const items = document.querySelector('#rq-review-items'); items.replaceChildren();
      selected.forEach(item => {
        const row = node('div', undefined, 'rq-review-item'); const text = node('div');
        text.append(node('strong', item.code), node('small', item.description));
        row.append(text, node('b', `${item.quantity} unid.`)); items.append(row);
      });
      document.querySelector('#rq-review-seller').textContent = `Vendedor: ${document.querySelector('#rq-seller').selectedOptions[0]?.textContent || ''}`;
      const note = document.querySelector('#rq-review-note'); note.textContent = document.querySelector('#rq-observation').value.trim(); note.hidden = !note.textContent;
      review.showModal(); return;
    }
    busy = true; confirm.disabled = true;
    const submit = document.querySelector('#rq-submit'); submit.disabled = true; submit.textContent = 'Enviando solicitud…';
  });
  window.addEventListener('pageshow', event => {
    if (event.persisted) {
      busy = false; confirmed = false; confirm.disabled = false;
      const submit = document.querySelector('#rq-submit'); submit.textContent = 'Revisar solicitud'; sync();
    }
  });
  sync();
})();
