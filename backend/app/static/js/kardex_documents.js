document.addEventListener('dblclick', (event) => {
  if (event.target.closest('a, button, input, select')) return;
  const row = event.target.closest('[data-movement-pdf]');
  if (row?.dataset.movementPdf) {
    window.open(row.dataset.movementPdf, '_blank', 'noopener');
  }
});
