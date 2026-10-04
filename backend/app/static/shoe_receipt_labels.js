(() => {
  const $=id=>document.getElementById(id);
  const {receipt_id:receiptId}=JSON.parse($('receipt-label-config').textContent);
  const checks=[...document.querySelectorAll('.receipt-variant-check')];
  const source=$('receipt-printer-source'),printer=$('receipt-printer'),status=$('receipt-job-status');
  let busy=false,loading=false;
  function selection(){return checks.filter(c=>c.checked);}
  function sync(){
    const selected=selection(),total=selected.reduce((sum,c)=>sum+Number(c.dataset.quantity),0);
    $('receipt-selected-total').textContent=`${total} etiquetas · ${selected.length} variantes seleccionadas`;
    const all=$('receipt-select-all');all.checked=checks.length>0&&selected.length===checks.length;all.indeterminate=selected.length>0&&selected.length<checks.length;
    document.querySelectorAll('.receipt-model-check').forEach(c=>{const group=checks.filter(v=>v.dataset.model===c.dataset.model),n=group.filter(v=>v.checked).length;c.checked=n===group.length;c.indeterminate=n>0&&n<group.length;});
    $('receipt-print').disabled=busy||loading||!total||total>5000||!printer.value;
    $('receipt-preview').disabled=busy||loading||!total||total>5000;
    [source,printer,$('receipt-connect'),all,$('receipt-show-price'),...checks,...document.querySelectorAll('.receipt-model-check')].forEach(el=>el.disabled=busy||loading);
    if(total>5000)status.textContent='Máximo 5,000 etiquetas por envío. Desmarque algunos modelos o variantes; las cantidades originales se conservan.';
  }
  checks.forEach(c=>c.addEventListener('change',()=>{status.textContent='';sync();}));
  $('receipt-select-all').addEventListener('change',e=>{checks.forEach(c=>c.checked=e.target.checked);status.textContent='';sync();});
  document.querySelectorAll('.receipt-model-check').forEach(c=>c.addEventListener('change',()=>{checks.filter(v=>v.dataset.model===c.dataset.model).forEach(v=>v.checked=c.checked);status.textContent='';sync();}));
  source.addEventListener('change',()=>{printer.innerHTML='<option value="">Conecte para elegir impresora</option>';sync();});
  printer.addEventListener('change',()=>{try{localStorage.setItem('miss-zapatos-printer-'+source.value,printer.value);}catch(_){}sync();});
  $('receipt-connect').addEventListener('click',async()=>{
    loading=true;sync();$('receipt-printer-status').textContent='Consultando impresoras…';
    try{
      let printers;
      if(source.value==='client')printers=await ShoeLabelPrinter.list();
      else{
        const response=await fetch('/inventory/etiquetas-zapatos/printers',{headers:{'X-Requested-With':'fetch'}});
        if(response.redirected)throw new Error('Inicie sesión nuevamente.');
        const data=await response.json();if(!response.ok)throw new Error(data.detail||'No se pudieron consultar las impresoras');printers=data.printers;
      }
      printer.innerHTML='<option value="">Seleccione una impresora</option>';
      for(const p of printers){const option=new Option(p.name+(p.enabled?'':' (deshabilitada)'),p.id);option.disabled=!p.enabled;printer.add(option);}
      let saved;try{saved=localStorage.getItem('miss-zapatos-printer-'+source.value);}catch(_){}
      if([...printer.options].some(o=>o.value===saved&&!o.disabled))printer.value=saved;
      $('receipt-printer-status').textContent=printers.length?'Seleccione el destino del envío.':'No hay impresoras disponibles. Puede abrir el PDF.';
    }catch(err){printer.innerHTML='<option value="">Impresión directa no disponible</option>';$('receipt-printer-status').textContent=err.message;}
    finally{loading=false;sync();}
  });
  async function send(preview){
    if(busy||loading||(preview?$('receipt-preview'):$('receipt-print')).disabled)return;
    const tab=preview?window.open('','_blank'):null;
    if(preview&&!tab){status.textContent='Permita ventanas emergentes para ver el PDF.';return;}
    const server=!preview&&source.value==='server';
    const payload={variant_ids:selection().map(c=>Number(c.dataset.variant)),mode:server?'direct':'pdf',printer:printer.value,show_price:$('receipt-show-price').checked};
    busy=true;sync();status.textContent=preview?'Generando PDF con las cantidades del ingreso…':'Preparando envío a la impresora…';
    try{
      const response=await fetch(`/inventory/ingresos/${receiptId}/labels/jobs`,{method:'POST',headers:{'Content-Type':'application/json','X-Requested-With':'fetch'},body:JSON.stringify(payload)});
      if(response.redirected)throw new Error('Su sesión expiró. Inicie sesión nuevamente.');
      if(!response.ok){const data=await response.json();throw new Error(data.detail||'No se pudo generar el trabajo');}
      if(server){const data=await response.json();status.textContent=`${data.total} etiquetas enviadas. Trabajo ${data.job}. Revise la salida física.`;}
      else{
        const blob=await response.blob(),total=response.headers.get('X-Total-Labels');
        if(preview){const url=URL.createObjectURL(blob);tab.location.href=url;setTimeout(()=>URL.revokeObjectURL(url),300000);status.textContent=`PDF listo: ${total} etiquetas, según las cantidades del ingreso.`;}
        else{await ShoeLabelPrinter.print(blob,payload.printer,Number(response.headers.get('X-Label-Width-Mm')),Number(response.headers.get('X-Label-Height-Mm')));status.textContent=`${total} etiquetas enviadas a ${payload.printer}. Revise la salida física.`;}
      }
    }catch(err){if(tab)tab.close();status.textContent=err.message+(preview?'':' Revise la cola antes de volver a enviar.');}
    finally{busy=false;sync();}
  }
  $('receipt-print').addEventListener('click',()=>send(false));$('receipt-preview').addEventListener('click',()=>send(true));sync();
})();
