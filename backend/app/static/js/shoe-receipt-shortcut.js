(()=>{'use strict';
  const links=document.querySelectorAll('[data-receipt-origin-selector], [data-receipt-warehouse]');
  function update(link){
    const selector=link.dataset.receiptOriginSelector;
    const field=selector?document.querySelector(selector):null;
    const warehouse=field?.value||link.dataset.receiptWarehouse||'';
    if(!/^\d+$/.test(warehouse))return false;
    const url=new URL('/inventory/ingresos-unidad',location.origin);
    url.searchParams.set('bodega_id',warehouse);
    link.href=url.pathname+url.search;
    return true;
  }
  for(const link of links){
    update(link);
    link.addEventListener('click',event=>{if(!update(link)){event.preventDefault();alert('Selecciona la bodega de origen antes de solicitar el ingreso.');}});
  }
  document.addEventListener('change',()=>{for(const link of links)update(link);});
})();
