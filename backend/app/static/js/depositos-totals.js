(() => {
  'use strict';
  const table = document.querySelector('#depositos-list');
  if (!table) return;
  const cents = value => {
    const match = String(value ?? '').trim().match(/^(-?)(\d+)(?:\.(\d{1,2}))?$/);
    if (!match) return null;
    const number = Number(match[2]) * 100 + Number((match[3] || '').padEnd(2, '0'));
    return Number.isSafeInteger(number) ? (match[1] ? -number : number) : null;
  };
  const format = value => (value / 100).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  function sumRows() {
    let cs = 0, usd = 0;
    for (const row of table.querySelectorAll('tbody tr[data-id]')) {
      const c = cents(row.dataset.sumCs), u = cents(row.dataset.sumUsd);
      if (c === null || u === null || !Number.isSafeInteger(cs + c) || !Number.isSafeInteger(usd + u)) {
        document.querySelector('#depositos-sum-status').textContent = 'No se pudo verificar la autosuma. Recarga el listado para consultar los totales del servidor.';
        return;
      }
      cs += c; usd += u;
    }
    document.querySelector('#depositos-footer-cs').textContent = `C$ ${format(cs)}`;
    document.querySelector('#depositos-footer-usd').textContent = `US$ ${format(usd)}`;
    document.querySelector('#depositos-kpi-cs').textContent = `C$ ${format(cs)}`;
    document.querySelector('#depositos-kpi-usd').textContent = `US$ ${format(usd)}`;
  }
  function refresh() {
    const url = new URL(window.location.href);
    url.searchParams.delete('success'); url.searchParams.delete('error');
    url.searchParams.set('_refresh', String(Date.now()));
    window.location.replace(url.href);
  }
  document.querySelector('#depositos-refresh')?.addEventListener('click', event => { event.preventDefault(); refresh(); });
  window.addEventListener('pageshow', event => { if (event.persisted) refresh(); });
  new MutationObserver(sumRows).observe(table.tBodies[0], {childList: true, subtree: true, attributes: true, attributeFilter: ['data-sum-cs', 'data-sum-usd']});
  sumRows();
})();
