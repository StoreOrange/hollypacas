(() => {
  'use strict';
  function start() {
    const dataElement = document.getElementById('accountingManualData');
    if (!dataElement) return;
    const data = JSON.parse(dataElement.textContent);
    const form = document.getElementById('entryForm');
    const body = document.getElementById('linesBody');
    const accounts = new Map(data.accounts.map(a => [String(a.id), a]));
    const labels = new Map(data.accounts.flatMap(a => [[`${a.codigo} · ${a.nombre}`, a], [a.codigo, a]]));
    const currency = document.getElementById('entryCurrency');
    const rate = document.getElementById('manualRate');
    const journal = document.getElementById('entryJournal');
    const errorBox = document.getElementById('entryError');
    const numberFormat = new Intl.NumberFormat('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    let rowSequence = 0;
    const normalize = value => String(value||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
    const amount = input => window.AccountingNumber.parse(input.value || 0);
    const cents = value => Math.round((value + Number.EPSILON) * 100);
    function showError(message, focus) {
      errorBox.textContent = message;
      errorBox.hidden = false;
      focus?.focus();
    }
    function accountFor(row) { return labels.get(row.querySelector('.line-account').value.trim()); }
    function update() {
      let debit = 0, credit = 0, count = 0, valid = true;
      for (const row of body.rows) {
        const d = amount(row.querySelector('.line-debit'));
        const c = amount(row.querySelector('.line-credit'));
        if (d || c) {
          count++;
          valid &&= !!accountFor(row) && Number.isFinite(d) && Number.isFinite(c) && d >= 0 && c >= 0 && !(d > 0 && c > 0);
        }
        debit += cents(d); credit += cents(c);
      }
      document.getElementById('totalDebit').textContent = numberFormat.format(debit / 100);
      document.getElementById('totalCredit').textContent = numberFormat.format(credit / 100);
      document.querySelectorAll('.currency-label').forEach(el => el.textContent = currency.value === 'USD' ? 'USD' : 'C$');
      const balanced = valid && count >= 2 && debit > 0 && debit === credit;
      const badge = document.getElementById('balanceStatus');
      badge.classList.toggle('balanced', balanced);
      badge.textContent = balanced ? 'Cuadrado · listo para publicar' : !count ? 'Agrega las líneas del comprobante' : debit !== credit ? `Diferencia: ${currency.value === 'USD' ? 'USD' : 'C$'} ${numberFormat.format(Math.abs(debit-credit)/100)}` : 'Revisa las cuentas y montos';
      document.getElementById('publishEntry').disabled = !balanced || (currency.value === 'USD' && !(amount(rate) > 0));
      document.getElementById('rateField').hidden = currency.value !== 'USD';
      rate.required = currency.value === 'USD';
      const hint = document.getElementById('conversionHint');
      hint.hidden = currency.value !== 'USD';
      if (!hint.hidden) hint.textContent = `Los libros se registran en C$. Tasa aplicada: ${amount(rate) || 'pendiente'}.`;
    }
    function addLine(saved = {}, focus = false) {
      const row = body.insertRow();
      // Structure is static; account names and user descriptions use value/textContent.
      row.innerHTML = '<td class="account-cell" data-label="Cuenta contable"><div class="account-picker"><input class="form-control line-account" placeholder="Código o nombre de la cuenta" autocomplete="off" aria-label="Cuenta contable" role="combobox" aria-autocomplete="list" aria-expanded="false"><div class="account-options" role="listbox" hidden></div></div><div class="line-hint account-hint"></div></td><td class="detail-cell" data-label="Detalle"><input class="form-control line-detail" placeholder="Detalle de esta línea" maxlength="200" aria-label="Detalle de la línea"></td><td data-label="Debe"><input class="form-control money-input line-debit" type="number" min="0" step="0.01" inputmode="decimal" placeholder="0.00" aria-label="Debe"></td><td data-label="Haber"><input class="form-control money-input line-credit" type="number" min="0" step="0.01" inputmode="decimal" placeholder="0.00" aria-label="Haber"></td><td class="remove-cell"><button type="button" class="remove-line" title="Eliminar línea" aria-label="Eliminar línea"><i class="bi bi-x-lg"></i></button></td>';
      const accountInput = row.querySelector('.line-account');
      const account = accounts.get(String(saved.cuenta_id));
      accountInput.value = account ? `${account.codigo} · ${account.nombre}` : '';
      row.querySelector('.line-detail').value = saved.descripcion || '';
      const divisor = data.editing && currency.value === 'USD' ? amount(rate) || 1 : 1;
      row.querySelector('.line-debit').value = saved.debe ? (saved.debe/divisor).toFixed(2) : '';
      row.querySelector('.line-credit').value = saved.haber ? (saved.haber/divisor).toFixed(2) : '';
      row.querySelectorAll('.money-input').forEach(input=>window.AccountingNumber.bind(input));
      const menu = row.querySelector('.account-options');
      menu.id = `account-options-${++rowSequence}`;
      accountInput.setAttribute('aria-controls',menu.id);
      let options = [], activeIndex = -1;
      function hideOptions() { menu.hidden = true; accountInput.setAttribute('aria-expanded','false'); accountInput.removeAttribute('aria-activedescendant'); }
      function resolve() {
        const selected = accountFor(row);
        row.dataset.accountId = selected ? String(selected.id) : '';
        row.querySelector('.account-hint').textContent = selected ? selected.path || selected.nombre : accountInput.value ? 'Selecciona una cuenta del catálogo' : '';
        accountInput.classList.toggle('invalid-account', !!accountInput.value && !selected);
        update();
      }
      function choose(selected) {
        if (!selected) return;
        accountInput.value = `${selected.codigo} · ${selected.nombre}`;
        hideOptions(); resolve(); row.querySelector('.line-detail').focus();
      }
      function highlight() {
        [...menu.children].forEach((button,index) => {button.classList.toggle('active',index===activeIndex);button.setAttribute('aria-selected',String(index===activeIndex));});
        const button = menu.children[activeIndex];
        if (button) { accountInput.setAttribute('aria-activedescendant',button.id); button.scrollIntoView({block:'nearest'}); }
      }
      function showOptions() {
        const query = normalize(accountInput.value.trim()), terms = query.split(/\s+/).filter(Boolean);
        options = data.accounts.filter(a=>terms.every(term=>normalize(`${a.codigo} ${a.nombre} ${a.descripcion||''} ${a.path||''}`).includes(term)))
          .sort((a,b)=>(normalize(b.codigo)===query?1:0)-(normalize(a.codigo)===query?1:0)||a.codigo.localeCompare(b.codigo)).slice(0,25);
        menu.replaceChildren();
        options.forEach((selected,index)=>{
          const button = document.createElement('button');button.type='button';button.className='account-option';button.id=`${menu.id}-${selected.id}`;button.setAttribute('role','option');
          const title = document.createElement('strong');title.textContent=`${selected.codigo} · ${selected.nombre}`;
          const path = document.createElement('small');path.textContent=selected.path||selected.descripcion||'';
          button.append(title,path);button.addEventListener('mousedown',event=>event.preventDefault());button.addEventListener('click',()=>choose(selected));menu.append(button);
        });
        activeIndex=options.length?0:-1;menu.hidden=!options.length;accountInput.setAttribute('aria-expanded',String(options.length>0));highlight();
      }
      accountInput.addEventListener('input',()=>{resolve();showOptions();});
      accountInput.addEventListener('focus',()=>{if(accountInput.value.trim())showOptions();});
      accountInput.addEventListener('change',resolve);
      accountInput.addEventListener('blur',()=>setTimeout(hideOptions,100));
      function nextRow() {
        const next=row.nextElementSibling;
        if(next) next.querySelector('.line-account').focus(); else addLine({},true);
      }
      accountInput.addEventListener('keydown',event=>{
        if(event.key==='Escape'){hideOptions();return;}
        if(event.key==='ArrowDown'||event.key==='ArrowUp'){
          event.preventDefault();if(menu.hidden)showOptions();else{activeIndex=Math.max(0,Math.min(options.length-1,activeIndex+(event.key==='ArrowDown'?1:-1)));highlight();}return;
        }
        if(event.key==='Enter'){
          event.preventDefault();
          if(event.shiftKey){const previous=row.previousElementSibling;if(previous)previous.querySelector('.line-credit').focus();return;}
          const selected=(!menu.hidden?options[activeIndex]:null)||accountFor(row);
          if(selected)choose(selected);else{showOptions();showError('Elige una cuenta con las flechas y confirma con Enter.',accountInput);}
        }
      });
      row.querySelector('.line-detail').addEventListener('keydown',event=>{if(event.key==='Enter'){event.preventDefault();(event.shiftKey?accountInput:row.querySelector('.line-debit')).focus();}});
      row.querySelectorAll('.money-input').forEach(input=>input.addEventListener('keydown',event=>{
        if(event.key!=='Enter')return;event.preventDefault();
        if(event.shiftKey){(input.classList.contains('line-debit')?row.querySelector('.line-detail'):row.querySelector('.line-debit')).focus();return;}
        if(!input.checkValidity()){input.reportValidity();return;}
        if(input.value.trim())input.value=numberFormat.format(amount(input));
        if(input.classList.contains('line-debit')){row.querySelector('.line-credit').focus();return;}
        if(!(amount(row.querySelector('.line-debit'))>0||amount(row.querySelector('.line-credit'))>0)){showError('Escribe un monto en Debe o Haber antes de avanzar.',input);return;}
        nextRow();
      }));
      row.querySelectorAll('.money-input').forEach(input => input.addEventListener('input', () => {
        // A line is either a debit or a credit. Switch sides without leaving
        // an accidental amount on both sides.
        if (amount(input) > 0) row.querySelector(input.classList.contains('line-debit') ? '.line-credit' : '.line-debit').value = '';
        errorBox.hidden = true;
        update();
      }));
      row.querySelectorAll('.line-detail,.money-input').forEach(input=>input.addEventListener('focus',()=>input.select()));
      row.querySelector('.remove-line').addEventListener('click', () => { row.remove(); if (!body.rows.length) addLine(); update(); });
      resolve();
      if (focus) accountInput.focus();
    }
    document.getElementById('addLine').addEventListener('click', () => addLine({}, true));
    currency.addEventListener('change', update);
    rate.addEventListener('input', update);
    journal.addEventListener('change', () => {
      const option = journal.selectedOptions[0];
      document.getElementById('voucherTypeSelect').value = option?.dataset.voucher || '';
      if (option?.dataset.branch) document.getElementById('entryBranch').value = option.dataset.branch;
      if (option?.dataset.currency) currency.value = option.dataset.currency;
      update();
    });
    document.getElementById('entryClient').addEventListener('change', event => { if (event.target.value) document.getElementById('entrySupplier').value = ''; });
    document.getElementById('entrySupplier').addEventListener('change', event => { if (event.target.value) document.getElementById('entryClient').value = ''; });
    form.addEventListener('submit', event => {
      errorBox.hidden = true;
      const state = event.submitter?.dataset.state || 'BORRADOR';
      document.getElementById('entryStatus').value = state;
      const payload = document.getElementById('linePayload'); payload.replaceChildren();
      const items = [];
      for (const row of body.rows) {
        const d = amount(row.querySelector('.line-debit')), c = amount(row.querySelector('.line-credit'));
        const input = row.querySelector('.line-account');
        if (!input.value.trim() && !d && !c && !row.querySelector('.line-detail').value.trim()) continue;
        const selected = accountFor(row);
        if (!selected || !Number.isFinite(d) || !Number.isFinite(c) || d < 0 || c < 0 || (!d && !c) || (d && c)) {
          event.preventDefault(); showError('Cada línea requiere una cuenta del catálogo y un monto positivo en Debe o Haber.', input); return;
        }
        items.push({account:selected.id, detail:row.querySelector('.line-detail').value, debit:d, credit:c});
      }
      if (!items.length) { event.preventDefault(); showError('Agrega al menos una línea para guardar el borrador.'); return; }
      const debit = items.reduce((sum,row) => sum+cents(row.debit),0), credit = items.reduce((sum,row) => sum+cents(row.credit),0);
      if (state === 'POSTEADO' && (items.length < 2 || !debit || debit !== credit)) { event.preventDefault(); showError('Para publicar, Debe y Haber deben cuadrar y tener al menos dos líneas.'); return; }
      for (const item of items) {
        for (const [name,value] of Object.entries({line_cuenta_id:item.account,line_descripcion:item.detail,line_debe:item.debit.toFixed(2),line_haber:item.credit.toFixed(2)})) {
          const input = document.createElement('input'); input.type = 'hidden'; input.name = name; input.value = value; payload.append(input);
        }
      }
    });
    let refreshing = false;
    async function refreshAccounts() {
      if(refreshing)return;refreshing=true;
      const status=document.getElementById('catalogStatus');
      try{
        const response=await fetch('/accounting/accounts/options',{headers:{Accept:'application/json'}});if(!response.ok)throw new Error();
        const result=await response.json();
        const selected=[...body.rows].map(row=>[row,accountFor(row)?.id||row.dataset.accountId]);
        data.accounts=result.accounts;accounts.clear();labels.clear();
        result.accounts.forEach(a=>{accounts.set(String(a.id),a);labels.set(`${a.codigo} · ${a.nombre}`,a);labels.set(a.codigo,a);});
        selected.forEach(([row,id])=>{const a=accounts.get(String(id));if(a){row.querySelector('.line-account').value=`${a.codigo} · ${a.nombre}`;row.querySelector('.account-hint').textContent=a.path||a.nombre;}else if(id){row.querySelector('.account-hint').textContent='Esta cuenta ya no está activa; selecciona otra cuenta.';row.querySelector('.line-account').classList.add('invalid-account');}});
        document.querySelectorAll('.account-options').forEach(menu=>menu.hidden=true);
        status.textContent='Catálogo actualizado';update();document.dispatchEvent(new Event('accounting-catalog-refreshed'));
      }catch(e){status.textContent='No se pudo actualizar el catálogo. Intenta nuevamente.';}finally{refreshing=false;}
    }
    document.getElementById('refreshAccounts').addEventListener('click',refreshAccounts);
    window.addEventListener('focus',refreshAccounts);
    window.addEventListener('storage',event=>{if(event.key==='accounting-catalog-updated')refreshAccounts();});
    const visible = input => input && !input.disabled && !input.readOnly && !input.closest('[hidden]') && !input.closest('details:not([open])') && input.getClientRects().length > 0;
    form.addEventListener('keydown',event=>{
      if(event.isComposing)return;
      if(event.key==='Enter'&&(event.ctrlKey||event.metaKey)){
        event.preventDefault();event.stopPropagation();
        if(!document.getElementById('publishEntry').disabled)document.getElementById('publishEntry').focus();
        else showError('Revisa el cuadre, las cuentas y la tasa antes de publicar.');
        return;
      }
      if(event.key!=='Enter'||event.target.closest('tr')||!event.target.matches('input:not([type="hidden"]),select,textarea'))return;
      event.preventDefault();
      if(!event.target.checkValidity()){event.target.reportValidity();return;}
      const order=['entryDate','entryReference','entryDescription','entrySupplier','entryBranch','entryCurrency','manualRate','schoolOperation','schoolAmount','schoolPayment','schoolPaid','schoolDebit','schoolCredit','schoolPayable','schoolSource','schoolPreview'];
      let fields=order.map(id=>document.getElementById(id)).filter(visible);
      if(!fields.includes(event.target))fields=[...form.querySelectorAll('input,select,textarea,button[type="button"]')].filter(visible);
      const index=fields.indexOf(event.target);
      const next=fields[index+(event.shiftKey?-1:1)];
      if(next){next.focus();if(next.tagName==='INPUT'&&!['date','number'].includes(next.type))next.select();}
    }, true);
    if(window.AccountingSchool&&document.getElementById('schoolPanel')){
      const school=window.AccountingSchool, field=name=>document.getElementById(({Concept:'entryDescription',Reference:'entryReference'})[name]||'school'+name);
      const escape=value=>String(value??'').replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
      let proposal=null;
      const map={production_inventory:['inventario de produccion en proceso'],inventory:['inventarios'],expense:['gastos operativos'],asset:['mobiliario y equipo'],sale:['caja'],supplier:['proveedores'],collection:['caja'],transfer:['bancos'],capital:['caja'],loan:['bancos'],shortage:['otros gastos'],depreciation:['gasto de depreciacion','gastos de depreciacion'],quota:['impuestos y tasas','tributos','otros gastos'],advance:['anticipos a proveedores']};
      function selectSuggested(name,words,requiredType){
        const list=[...accounts.values()].filter(a=>!requiredType||a.tipo===requiredType);
        const chosen=words.map(word=>list.find(a=>normalize(a.nombre)===word)||list.find(a=>normalize(a.nombre).includes(word))).find(Boolean);
        field(name).value=chosen?String(chosen.id):'';
      }
      function fillSchoolAccounts(){
        ['Debit','Credit','Payable','CostAccount','InventoryAccount'].forEach(name=>{
          const value=field(name).value;field(name).innerHTML='<option value="">Revisa y selecciona una cuenta</option>'+[...accounts.values()].map(a=>`<option value="${escape(a.id)}">${escape(a.codigo)} · ${escape(a.nombre)}</option>`).join('');
          field(name).value=accounts.has(value)?value:'';
        });
      }
      function invalidate(){proposal=null;field('Apply').disabled=true;field('Proposal').hidden=true;field('Message').textContent='';}
      function lesson(){
        invalidate();const op=field('Operation').value,info=school.lessons[op];let payment=field('Payment').value;
        if((payment==='partial'&&!['inventory','production_inventory','expense','asset'].includes(op))||(payment==='credit'&&!['inventory','production_inventory','expense','asset','sale'].includes(op))){payment='cash';field('Payment').value='cash';}
        field('Explanation').textContent=info.explain;field('Support').textContent='Soporte: '+info.support;field('Rule').textContent=info.rule;
        field('DebitLabel').textContent='Debe · '+info.debit;field('CreditLabel').textContent='Haber · '+info.credit;
        const acquisition=['inventory','production_inventory','expense','asset'].includes(op),partial=acquisition&&payment==='partial';
        field('Payment').querySelector('option[value="credit"]').disabled=!acquisition&&op!=='sale';
        field('Payment').querySelector('option[value="partial"]').disabled=!acquisition;
        field('Payment').disabled=!acquisition&&!['sale','supplier','collection','capital','loan','quota','advance'].includes(op);
        ['PaidWrap','PayableWrap'].forEach(name=>field(name).hidden=!partial);
        ['CostWrap','CostAccountWrap','InventoryAccountWrap'].forEach(name=>field(name).hidden=op!=='sale');
        selectSuggested('Debit',map[op],['expense','shortage','depreciation','quota'].includes(op)?'RESULTADO':'BALANCE');
        if(op==='production_inventory'){
          const account=[...accounts.values()].find(a=>a.codigo==='1112'&&a.tipo==='BALANCE'&&a.naturaleza==='DEBE'&&normalize(a.nombre)==='inventario de produccion en proceso');
          field('Debit').value=account?String(account.id):'';
        }
        field('Debit').disabled=op==='production_inventory';
        if(['collection','capital','loan'].includes(op))selectSuggested('Debit',payment==='bank'?['bancos','banco']:['caja','efectivo'],'BALANCE');
        const cash=payment==='bank'?['bancos','banco']:['caja','efectivo'];
        if(acquisition)selectSuggested('Credit',payment==='credit'?['proveedores']:cash,'BALANCE');
        else if(op==='sale'){selectSuggested('Debit',payment==='credit'?['cuentas por cobrar clientes','clientes']:cash);selectSuggested('Credit',['ingresos por venta','ventas'],'RESULTADO');}
        else selectSuggested('Credit',({collection:['cuentas por cobrar clientes'],transfer:['caja'],capital:['aportacion de capital'],loan:['prestamos por pagar'],shortage:['inventarios'],depreciation:['depreciacion acumulada'],supplier:cash,quota:cash,advance:cash})[op]||cash,'BALANCE');
        selectSuggested('Payable',['proveedores'],'BALANCE');selectSuggested('CostAccount',['costo de ventas'],'RESULTADO');selectSuggested('InventoryAccount',['inventarios'],'BALANCE');
      }
      field('Suggest').addEventListener('click',()=>{
        let operation=school.classify(field('Concept').value);
        if(operation==='inventory'&&field('Operation').querySelector('option[value="production_inventory"]'))operation='production_inventory';
        if(operation){field('Operation').value=operation;lesson();
          if(operation==='expense'){const text=normalize(field('Concept').value);const categories=[[/alquiler/,['alquileres']], [/energia|electricidad/,['energia electrica']], [/agua/,['agua potable']], [/internet|telefono/,['telefonia e internet']], [/papeleria|utiles/,['papeleria y utiles']]];const match=categories.find(([regex])=>regex.test(text));if(match)selectSuggested('Debit',match[1],'RESULTADO');}
          field('Message').textContent='Orientación sugerida: '+school.lessons[operation].title+'. Confirma las cuentas y el importe.';}
        else field('Message').textContent='No hay una clasificación única. Selecciona qué ocurrió; puedes combinar varias líneas manualmente.';
      });
      field('Operation').addEventListener('change',lesson);field('Payment').addEventListener('change',lesson);
      [...document.getElementById('schoolPanel').querySelectorAll('input,select'),field('Concept'),field('Reference')].forEach(input=>input.addEventListener('input',invalidate));
      field('Preview').addEventListener('click',()=>{
        invalidate();
        try{
          if(field('Source').value!=='manual')throw Error(field('Source').value==='system'?'Revisa el asiento automático en el historial antes de registrar otro; esta propuesta no se carga para evitar duplicarlo.':'Indica si la operación ya está contabilizada.');
          if(!field('Reference').value.trim())throw Error('Indica el documento o referencia del soporte.');
          if(field('Operation').value==='production_inventory'){
            const account=accounts.get(field('Debit').value);
            if(!account||account.codigo!=='1112'||account.tipo!=='BALANCE'||account.naturaleza!=='DEBE'||normalize(account.nombre)!=='inventario de produccion en proceso')throw Error('La cuenta 1112 de producción en proceso no está disponible. Actualiza el catálogo o consulta al administrador.');
          }
          proposal=school.propose({operation:field('Operation').value,total:field('Amount').value,payment:field('Payment').value,debit:field('Debit').value,credit:field('Credit').value,paid:field('Paid').value,payable:field('Payable').value,cost:field('Cost').value,costAccount:field('CostAccount').value,inventoryAccount:field('InventoryAccount').value});
          if(proposal.some(line=>!accounts.has(String(line.cuenta_id))))throw Error('Una cuenta ya no está disponible. Actualiza el catálogo y revísala.');
          field('Proposal').innerHTML='<table class="table table-sm"><thead><tr><th>Cuenta</th><th>Debe</th><th>Haber</th></tr></thead><tbody>'+proposal.map(line=>{const a=accounts.get(String(line.cuenta_id));return `<tr><td>${escape(a.codigo)} · ${escape(a.nombre)}</td><td>${numberFormat.format(line.debe)}</td><td>${numberFormat.format(line.haber)}</td></tr>`;}).join('')+'</tbody></table>';
          field('Proposal').hidden=false;field('Apply').disabled=false;field('Apply').focus();field('Message').textContent='Propuesta en '+currency.value+'. Revisa las cuentas: cargarla solo prepara líneas; publicar requiere tu revisión.';
        }catch(error){field('Message').textContent=error.message;proposal=null;}
      });
      field('Apply').addEventListener('click',()=>{
        if(!proposal)return;
        if([...body.rows].some(row=>row.querySelector('.line-account').value.trim()||row.querySelector('.line-detail').value.trim()||amount(row.querySelector('.line-debit'))||amount(row.querySelector('.line-credit')))){
          field('Message').textContent='Ya hay líneas digitadas. Conserva esas líneas o usa Nuevo / limpiar antes de cargar esta propuesta.';return;
        }
        body.innerHTML='';proposal.forEach(line=>{
          const factor=data.editing&&currency.value==='USD'?(amount(rate)||1):1;
          addLine({...line,debe:line.debe*factor,haber:line.haber*factor});
        });
        document.getElementById('entryDescription').value=field('Concept').value.trim()||school.lessons[field('Operation').value].title;
        document.getElementById('entryReference').value=field('Reference').value.trim();
        if(field('Operation').value==='production_inventory'&&!data.editing){
          const purchase=[...journal.options].find(option=>option.dataset.kind==='PURCHASE');
          if(purchase){const captureCurrency=currency.value;journal.value=purchase.value;journal.dispatchEvent(new Event('change'));currency.value=captureCurrency;}
        }
        update();field('Message').textContent='Líneas cargadas y editables. Revisa fecha, diario, sucursal, tercero y soporte antes de publicar.';
        body.rows[0]?.querySelector('.line-detail').focus();field('Apply').disabled=true;proposal=null;
      });
      currency.addEventListener('change',invalidate);rate.addEventListener('input',invalidate);
      document.addEventListener('accounting-catalog-refreshed',()=>{invalidate();fillSchoolAccounts();});
      fillSchoolAccounts();lesson();
    }
    const registerTab = document.getElementById('registerTab'), historyTab = document.getElementById('historyTab');
    function switchTab(history) {
      document.getElementById('registerPanel').hidden = history;
      document.getElementById('historyPanel').hidden = !history;
      registerTab.classList.toggle('active', !history); historyTab.classList.toggle('active', history);
      registerTab.setAttribute('aria-selected', String(!history)); historyTab.setAttribute('aria-selected', String(history));
    }
    registerTab.addEventListener('click', () => switchTab(false)); historyTab.addEventListener('click', () => switchTab(true));
    document.querySelectorAll('.reverse-entry').forEach(button => button.addEventListener('click', () => {
      document.getElementById('reverseForm').action = `/accounting/entries/${button.dataset.entry}/reverse`;
      document.getElementById('reverseTitle').textContent = `Revertir ${button.dataset.number}`;
      document.getElementById('reverseDate').min = button.dataset.date;
      bootstrap.Modal.getOrCreateInstance(document.getElementById('reverseModal')).show();
    }));
    if (!data.editing) document.getElementById('voucherTypeSelect').value = journal.selectedOptions[0]?.dataset.voucher || document.getElementById('voucherTypeSelect').value;
    if (!data.editing && new URLSearchParams(location.search).has('journal_id')) journal.dispatchEvent(new Event('change'));
    (data.lines.length ? data.lines : [{},{}]).forEach(line => addLine(line));
    switchTab(!!data.history);
    const previewElement=document.getElementById('voucherPreviewModal');
    const viewer=window.AccountingVoucherPreview(previewElement);
    document.querySelectorAll('[data-voucher-preview]').forEach(link=>link.addEventListener('click',event=>{event.preventDefault();viewer.open(link.dataset.voucherPreview);}));
    previewElement.addEventListener('hidden.bs.modal',()=>{
      const clean=new URL(location.href);clean.searchParams.delete('pdf_entry_id');clean.searchParams.delete('success');history.replaceState(null,'',clean);
      switchTab(false);document.getElementById('entryDate').focus();
    });
    if(data.pdfEntry)viewer.open(data.pdfEntry);
    update();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();
