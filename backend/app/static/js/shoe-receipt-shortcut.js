(() => {
  'use strict';
  const links = document.querySelectorAll('[data-receipt-origin-selector], [data-receipt-warehouse]');
  let dialog, frame, returnModal, trigger, saved = false;
  function update(link) {
    const selector = link.dataset.receiptOriginSelector;
    const warehouse = (selector ? document.querySelector(selector)?.value : '') || link.dataset.receiptWarehouse || '';
    if (!/^\d+$/.test(warehouse)) return false;
    const url = new URL('/inventory/ingresos-unidad', location.origin);
    url.searchParams.set('bodega_id', warehouse);
    const query = link.dataset.receiptQuerySelector ? document.querySelector(link.dataset.receiptQuerySelector)?.value.trim() : '';
    if (query) url.searchParams.set('q', query.slice(0, 120));
    link.href = url.pathname + url.search;
    return true;
  }
  function prepare() {
    if (dialog) return;
    dialog = document.createElement('div');
    dialog.id = 'shoe-receipt-modal';
    dialog.className = 'modal fade';
    dialog.tabIndex = -1;
    dialog.setAttribute('aria-labelledby', 'shoe-receipt-title');
    dialog.setAttribute('aria-hidden', 'true');
    dialog.innerHTML = `<div class="modal-dialog modal-xl modal-dialog-centered modal-fullscreen-lg-down"><div class="modal-content border-0 shadow-lg" style="border-radius:18px;overflow:hidden"><div class="modal-header bg-light"><div><h2 id="shoe-receipt-title" class="modal-title fs-5"><i class="bi bi-box-arrow-in-down text-success me-2"></i>Ingreso rápido de artículo</h2><p class="small text-muted mb-0">Busca el artículo, confirma color, talla y cantidad, y registra el ingreso.</p></div><button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Cerrar ingreso rápido"></button></div><div class="modal-body p-0"><iframe title="Ingreso guiado por modelo, color y talla" style="display:block;width:100%;height:75vh;border:0"></iframe></div><div class="modal-footer py-2"><button type="button" class="btn btn-outline-primary" data-bs-dismiss="modal"><i class="bi bi-arrow-return-left me-1"></i>Volver y continuar</button></div></div></div>`;
    document.body.append(dialog);
    frame = dialog.querySelector('iframe');
    dialog.addEventListener('hide.bs.modal', event => {
      try { if (frame.contentWindow.shoeReceiptCanClose?.() === false) event.preventDefault(); } catch (_) { /* Session redirect: permit closing. */ }
    });
    dialog.addEventListener('hidden.bs.modal', () => {
      frame.src = 'about:blank';
      const previous = returnModal;
      returnModal = null;
      if (previous) {
        previous.addEventListener('shown.bs.modal', () => {
          if (saved) (document.getElementById('variant-search-run') || document.getElementById('variant-search-btn'))?.click();
          trigger?.focus();
        }, {once:true});
        bootstrap.Modal.getOrCreateInstance(previous).show();
      } else trigger?.focus();
    });
    window.addEventListener('message', event => {
      if (event.origin !== location.origin || event.source !== frame.contentWindow || !['shoe-receipt-close', 'shoe-receipt-saved'].includes(event.data?.type)) return;
      if (event.data.type === 'shoe-receipt-saved') { saved = true; return; }
      saved = saved || event.data.saved === true;
      bootstrap.Modal.getOrCreateInstance(dialog).hide();
    });
  }
  for (const link of links) {
    update(link);
    link.addEventListener('click', event => {
      if (!update(link)) { event.preventDefault(); alert('Selecciona la bodega de origen antes de solicitar el ingreso.'); return; }
      if (event.ctrlKey || event.metaKey || event.shiftKey || !window.bootstrap?.Modal) return;
      event.preventDefault();
      prepare(); trigger = link; saved = false;
      const url = new URL(link.href); url.searchParams.set('embed', '1');
      const show = () => { frame.src = url.pathname + url.search; bootstrap.Modal.getOrCreateInstance(dialog).show(); };
      returnModal = link.closest('.modal.show');
      if (returnModal) {
        returnModal.addEventListener('hidden.bs.modal', show, {once:true});
        bootstrap.Modal.getOrCreateInstance(returnModal).hide();
      } else show();
    });
  }
  document.addEventListener('change', () => { for (const link of links) update(link); });
})();
