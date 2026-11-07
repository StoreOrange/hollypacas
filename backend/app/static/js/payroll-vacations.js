(() => {
  const form = document.querySelector('#vacationForm');
  if (!form) return;
  const { kind, employee_id: employee, payment_method: method, journal_id: journal, cash_bodega_id: warehouse } = form.elements;
  const section = document.querySelector('#vacationDraft');
  const acknowledgement = document.querySelector('#vacationDraftAck');
  function update() {
    const cash = kind.value === 'CASH';
    const branch = employee.selectedOptions[0]?.dataset.branch;
    const pending = employee.selectedOptions[0]?.dataset.settlement || '';
    form.querySelectorAll('[data-rest]').forEach(el => {
      el.hidden = cash;
      el.querySelectorAll('input,select').forEach(input => { input.disabled = cash; input.required = !cash; });
    });
    form.querySelectorAll('[data-cash]').forEach(el => {
      el.hidden = !cash;
      el.querySelectorAll('input,select').forEach(input => { input.disabled = !cash; input.required = cash; });
    });
    warehouse.required = cash && method.value === 'EFECTIVO';
    warehouse.disabled = !warehouse.required;
    for (const option of journal.options) {
      if (!option.value) continue;
      option.hidden = option.dataset.kind !== (method.value === 'EFECTIVO' ? 'CASH' : 'BANK') || !!(option.dataset.branch && option.dataset.branch !== branch);
      if (option.hidden && option.selected) journal.value = '';
    }
    const journals = [...journal.options].filter(option => option.value && !option.hidden);
    if (!journal.value && journals.length === 1) journal.value = journals[0].value;
    for (const option of warehouse.options) {
      if (!option.value) continue;
      option.hidden = option.dataset.branch !== branch;
      if (option.hidden && option.selected) warehouse.value = '';
    }
    const primary = [...warehouse.options].filter(option => option.value && !option.hidden && option.dataset.primary === '1');
    if (!warehouse.value && primary.length === 1) warehouse.value = primary[0].value;
    section.hidden = !cash || !pending;
    acknowledgement.disabled = !cash || !pending;
    acknowledgement.required = cash && !!pending;
    document.querySelector('#vacationDraftLabel').textContent = pending;
    document.querySelector('#vacationHint').textContent = cash
      ? 'El pago cancela vacaciones por pagar. En efectivo registra un egreso y recibo de la caja seleccionada; la caja debe estar abierta.'
      : 'El descanso reduce los días disponibles. Su salario sigue pagándose en la planilla habitual: no se registra otro pago de caja aquí.';
  }
  [kind, employee, method].forEach(el => el.addEventListener('change', () => {
    if (el === kind || el === employee) acknowledgement.checked = false;
    update();
  }));
  update();
  form.addEventListener('keydown', event => {
    if (event.key === 'Enter' && event.target.matches('input,select')) {
      event.preventDefault();
      const fields = [...form.querySelectorAll('input,select,button')].filter(el => !el.disabled && el.type !== 'hidden' && el.getClientRects().length);
      fields[fields.indexOf(event.target) + 1]?.focus();
    }
  });
  form.addEventListener('submit', () => {
    const button = form.querySelector('button');
    button.disabled = true; button.textContent = 'Registrando…';
  });
})();
