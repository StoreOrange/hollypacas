/* Presentation only: does not edit quantities, prices or submission data. */
(() => {
  'use strict';
  const start=()=>{
    const root=document.querySelector('.holl-preventa');if(!root)return;
    document.documentElement.classList.add('holl-preventa-active');
    document.body.classList.add('holl-preventa-active');
    const reduced=()=>matchMedia('(prefers-reduced-motion:reduce)').matches;
    const steps=[...root.querySelectorAll('[data-pv-step]')];
    const activate=id=>steps.forEach(button=>{if(button.dataset.pvStep===id)button.setAttribute('aria-current','step');else button.removeAttribute('aria-current');});
    steps.forEach(button=>button.addEventListener('click',()=>{
      const section=document.getElementById(button.dataset.pvStep);if(!section)return;
      activate(section.id);section.scrollIntoView({behavior:reduced()?'auto':'smooth',block:'start'});
      const target=section.querySelector('input:not([type=hidden]):not(:disabled),select:not(:disabled),button:not(:disabled)');
      target?.focus({preventScroll:true});
    }));
    root.addEventListener('focusin',event=>{const card=event.target.closest('#preventaClientCard,#preventaProductsCard,#preventaItemsCard');if(card)activate(card.id);});
    const icon=(name,className='')=>{const node=document.createElement('i');node.className='bi '+name+' '+className;node.setAttribute('aria-hidden','true');return node;};
    const decorateRow=row=>{
      if(row.dataset.pvDecorated)return;
      row.dataset.pvDecorated='1';
      row.querySelector('td:first-child>.fw-semibold')?.prepend(icon('bi-box-seam','pv-product-icon'));
      const labels=[['bi-123','Cantidad'],['bi-tag','Precio'],['bi-boxes','Existencia']];
      labels.forEach(([name,text],index)=>{const cell=row.children[index+1];if(!cell)return;const label=document.createElement('span');label.className='pv-item-label';label.append(icon(name),document.createTextNode(text));cell.prepend(label);});
      const remove=row.querySelector('.item-remove');if(remove){remove.setAttribute('aria-label','Quitar producto del pedido');remove.replaceChildren(icon('bi-trash3'));}
      row.querySelector('.item-qty')?.setAttribute('aria-label','Cantidad del producto');
    };
    const resultObservers=[];
    [['clienteSearchResults','bi-person-fill','blue'],['productSearchResults','bi-box-seam-fill','teal']].forEach(([id,name,tone])=>{
      const list=document.getElementById(id);if(!list)return;
      const decorate=()=>list.querySelectorAll(':scope>button:not([data-pv-decorated])').forEach(button=>{
        button.dataset.pvDecorated='1';button.classList.add('pv-result-option');
        const copy=document.createElement('span');copy.className='pv-result-copy';while(button.firstChild)copy.append(button.firstChild);
        const badge=document.createElement('span');badge.className='pv-result-icon '+tone;badge.append(icon(name));
        button.append(badge,copy);
      });
      const observer=new MutationObserver(decorate);observer.observe(list,{childList:true});resultObservers.push(observer);decorate();
    });
    window.addEventListener('pagehide',()=>resultObservers.forEach(observer=>observer.disconnect()),{once:true});
    const body=document.getElementById('itemsBody'),summary=root.querySelector('.draft-summary');
    if(body&&summary){
      body.querySelectorAll('tr[data-item-row]').forEach(decorateRow);
      let timer=null;
      const observer=new MutationObserver(mutations=>{
        mutations.forEach(m=>[...m.addedNodes].filter(node=>node.nodeType===1&&node.matches?.('tr[data-item-row]')).forEach(decorateRow));
        if(reduced()||!mutations.some(m=>m.type==='childList'&&[...m.addedNodes].some(node=>node.nodeType===1&&node.matches?.('tr[data-item-row]'))))return;
        summary.classList.remove('pv-summary-flash');requestAnimationFrame(()=>summary.classList.add('pv-summary-flash'));
        clearTimeout(timer);timer=setTimeout(()=>summary.classList.remove('pv-summary-flash'),500);
      });
      observer.observe(body,{childList:true});
      window.addEventListener('pagehide',()=>{observer.disconnect();clearTimeout(timer);},{once:true});
    }
  };
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start);else start();
})();
