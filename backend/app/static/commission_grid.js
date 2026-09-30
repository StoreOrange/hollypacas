(() => {
  'use strict';
  const shell=document.getElementById('commission-workbench');if(!shell)return;
  const el=id=>document.getElementById(id), table=shell.querySelector('.assignment-grid');
  const heads=[...table.tHead.rows[0].cells], cols=[...table.querySelectorAll('col')];
  const fields=['factura','cliente','descripcion','cant_vendida','cantidad','precio','comision_unit','comision_total','vendedor_facturacion','vendedor_asignado',null];
  const defaults=[100,160,260,85,95,115,120,125,155,170,95];
  const key='pacas:commission-grid:v1';let prefs={};
  try{prefs=JSON.parse(localStorage.getItem(key)||'{}')||{};}catch(_){}
  let widths=defaults.map((v,i)=>Math.max(70,Math.min(600,Number(prefs.widths?.[i])||v)));
  const locked=new Set([0,4,7,9,10]);
  let hidden=new Set(Array.isArray(prefs.hidden)?prefs.hidden.filter(i=>Number.isInteger(i)&&i>=0&&i<11&&!locked.has(i)):[]);
  const save=()=>{try{localStorage.setItem(key,JSON.stringify({widths,hidden:[...hidden],density:shell.dataset.density}));}catch(_){}};
  const refresh=()=>document.dispatchEvent(new CustomEvent('commission:grid-refresh'));
  const layout=()=>{
    const totalWidth=widths.reduce((n,w,i)=>n+(hidden.has(i)?0:w),0);
    table.style.width='100%';
    cols.forEach((col,i)=>{col.style.width=(100*widths[i]/totalWidth)+'%';col.style.display=hidden.has(i)?'none':'';});
    for(const row of table.rows){
      if(row.cells.length===11)[...row.cells].forEach((cell,i)=>{cell.style.display=hidden.has(i)?'none':'';if(row.hasAttribute('data-assignment-row'))cell.dataset.label=heads[i].querySelector('button')?.textContent||heads[i].textContent.trim();});
      else if(row.cells.length===1)row.cells[0].colSpan=11-hidden.size;
    }
    heads.forEach((head,i)=>{if(fields[i])head.setAttribute('aria-sort',el('assignment-grid-sort').value===fields[i]?(el('assignment-grid-dir').dataset.dir==='desc'?'descending':'ascending'):'none');});
  };
  heads.forEach((head,i)=>{
    const label=head.textContent.trim().replace(/\s+/g,' ');
    if(fields[i]){
      const button=document.createElement('button');button.type='button';button.className='commission-sort-header';button.textContent=label;
      button.addEventListener('click',()=>{const select=el('assignment-grid-sort');if(select.value===fields[i])el('assignment-grid-dir').click();else{select.value=fields[i];select.dispatchEvent(new Event('change'));}});
      head.replaceChildren(button);
    }
    const handle=document.createElement('span');handle.className='commission-resizer';handle.tabIndex=0;handle.role='separator';handle.setAttribute('aria-orientation','vertical');handle.setAttribute('aria-label',`Ancho de ${label}`);handle.setAttribute('aria-valuemin','70');handle.setAttribute('aria-valuemax','600');
    const resize=value=>{widths[i]=Math.max(70,Math.min(600,value));handle.setAttribute('aria-valuenow',widths[i]);layout();};
    handle.setAttribute('aria-valuenow',widths[i]);
    handle.addEventListener('pointerdown',event=>{
      if(event.button!==0)return;event.preventDefault();const start=event.clientX,width=widths[i];handle.setPointerCapture(event.pointerId);
      const move=e=>resize(width+e.clientX-start);
      const end=()=>{handle.removeEventListener('pointermove',move);handle.removeEventListener('pointerup',end);handle.removeEventListener('pointercancel',end);save();};
      handle.addEventListener('pointermove',move);handle.addEventListener('pointerup',end);handle.addEventListener('pointercancel',end);
    });
    handle.addEventListener('keydown',event=>{if(!['ArrowLeft','ArrowRight'].includes(event.key))return;event.preventDefault();resize(widths[i]+(event.key==='ArrowRight'?1:-1)*(event.shiftKey?30:10));save();});
    head.append(handle);
    const option=document.createElement('label'),check=document.createElement('input');check.type='checkbox';check.checked=!hidden.has(i);check.disabled=locked.has(i);
    check.addEventListener('change',()=>{if(check.checked)hidden.delete(i);else hidden.add(i);layout();save();});
    option.append(check,document.createTextNode(label));el('commission-column-options').append(option);
  });
  const reset=document.createElement('button');reset.type='button';reset.textContent='Restablecer columnas';reset.addEventListener('click',()=>{widths=[...defaults];hidden.clear();el('commission-column-options').querySelectorAll('input').forEach(c=>c.checked=true);heads.forEach((h,i)=>h.querySelector('.commission-resizer').setAttribute('aria-valuenow',widths[i]));layout();save();});el('commission-column-options').append(reset);
  shell.dataset.density=prefs.density==='comfortable'?'comfortable':'compact';el('commission-density').value=shell.dataset.density;
  el('commission-density').addEventListener('change',event=>{shell.dataset.density=event.target.value;save();});
  el('commission-sale-filter').addEventListener('change',refresh);
  el('commission-clear-search').addEventListener('click',()=>{el('assignment-grid-search').value='';el('commission-sale-filter').value='';el('commission-vendor-view').value='';refresh();});
  let savedScroll,previousOverflow;
  const focusButton=el('commission-focus');
  const focus=on=>{
    if(on){savedScroll=[window.scrollX,window.scrollY];previousOverflow=document.body.style.overflow;document.body.style.overflow='hidden';}
    shell.classList.toggle('commission-focused',on);focusButton.setAttribute('aria-pressed',String(on));focusButton.textContent=on?'Volver al tamaño normal':'Ampliar tabla';
    if(!on){document.body.style.overflow=previousOverflow||'';window.scrollTo(...(savedScroll||[0,0]));focusButton.focus({preventScroll:true});}
  };
  focusButton.addEventListener('click',()=>focus(!shell.classList.contains('commission-focused')));
  document.addEventListener('keydown',event=>{if(event.key==='Escape'&&shell.classList.contains('commission-focused'))focus(false);});
  table.addEventListener('keydown',event=>{
    if(!event.ctrlKey||!['ArrowUp','ArrowDown'].includes(event.key))return;
    const cell=event.target.closest('td'),row=cell?.closest('[data-assignment-row]');if(!row)return;
    const visible=[...table.querySelectorAll('[data-assignment-row]')].filter(r=>r.style.display!=='none');
    const next=visible[visible.indexOf(row)+(event.key==='ArrowDown'?1:-1)];
    const target=next?.cells[cell.cellIndex]?.querySelector('input,select,button');if(target){event.preventDefault();target.focus();}
  });
  document.addEventListener('commission:grid-rendered',layout);layout();
})();
