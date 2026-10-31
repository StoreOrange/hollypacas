(() => {
  'use strict';
  const form = document.getElementById('mc-payment');
  if (form) {
    const $ = id => document.getElementById('mc-'+id);
    let previewURL;
    function methodChanged() {
      const bank = $('method').value !== 'efectivo';
      $('bank-fields').hidden = !bank;
      ['bank','reference'].forEach(id => { $(id).required = bank; $(id).disabled = !bank; });
      $('support').required = bank;
      $('support-hint').textContent = bank ? 'Obligatoria para depósito o transferencia. JPG, PNG o WebP, hasta 8 MB.' : 'Puedes adjuntar una foto del recibo. Hasta 8 MB.';
    }
    function balanceHint() {
      const amount = Number($('amount').value);
      $('feedback').textContent = $('currency').value === form.dataset.currency && Number.isFinite(amount) && amount > 0
        ? 'Saldo después del abono: '+($('currency').value === 'USD' ? 'USD ' : 'C$ ')+(Number(form.dataset.balance)-amount).toLocaleString('es-NI',{minimumFractionDigits:2,maximumFractionDigits:2})
        : 'El saldo se validará con la tasa registrada para la fecha del pago.';
    }
    $('method').addEventListener('change',methodChanged); methodChanged();
    $('amount').addEventListener('input',balanceHint); $('currency').addEventListener('change',balanceHint);
    $('settle').addEventListener('click',() => { $('currency').value = form.dataset.currency; $('amount').value = form.dataset.balance; balanceHint(); $('amount').focus(); });
    $('support').addEventListener('change',() => {
      if (previewURL) URL.revokeObjectURL(previewURL);
      const file = $('support').files[0]; $('preview').hidden = true; $('support').setCustomValidity('');
      if (!file) return;
      if (file.size > 8*1024*1024 || !['image/jpeg','image/png','image/webp'].includes(file.type)) {
        $('support').setCustomValidity('Selecciona una foto JPG, PNG o WebP de hasta 8 MB.'); $('support').reportValidity(); return;
      }
      previewURL = URL.createObjectURL(file); $('preview').src = previewURL; $('preview').hidden = false;
    });
    form.addEventListener('submit',event => {
      if ($('currency').value === form.dataset.currency && Number($('amount').value) > Number(form.dataset.balance)) {
        event.preventDefault(); $('feedback').textContent = 'El monto supera el saldo pendiente.'; $('amount').focus(); return;
      }
      const button = form.querySelector('[type=submit]'); button.disabled = true; button.textContent = 'Registrando pago…';
    });
  }
  let dialog;
  document.querySelectorAll('[data-support]').forEach(link => link.addEventListener('click',event => {
    if (event.metaKey || event.ctrlKey || !window.HTMLDialogElement) return;
    event.preventDefault();
    if (!dialog) {
      dialog = document.createElement('dialog'); dialog.className = 'mc-lightbox';
      dialog.innerHTML = '<button type="button" class="btn btn-outline-secondary"><i class="bi bi-x-lg me-1"></i>Cerrar soporte</button><img alt="Imagen de soporte del pago">';
      document.body.append(dialog); dialog.querySelector('button').addEventListener('click',()=>dialog.close());
    }
    dialog.querySelector('img').src = link.href; dialog.showModal();
  }));
})();
