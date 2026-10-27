(() => {
  'use strict';
  const root=document.querySelector('.holl-home');if(!root)return;
  const query=root.querySelector('#hollHomeSearch');
  const actions=[...root.querySelectorAll('[data-home-action]')];
  const sections=[...root.querySelectorAll('[data-home-section]')];
  const buttons=[...root.querySelectorAll('[data-home-filter]')];
  const clear=root.querySelector('#hollHomeClear');
  const normalize=value=>String(value).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
  const catalog=actions.map(element=>({element,text:normalize(element.textContent+' '+element.dataset.homeSearch),kind:element.dataset.homeKind}));
  const stops=new Set(['quiero','necesito','un','una','el','la','de','del','para','ver','ir','a','los','las']);
  let kind='all';
  document.documentElement.classList.add('holl-home-active');
  function update(){
    const terms=normalize(query.value).split(/\s+/).filter(term=>term&&!stops.has(term));
    let visible=0;
    catalog.forEach(item=>{item.element.hidden=(kind!=='all'&&kind!==item.kind)||!terms.every(term=>item.text.includes(term));if(!item.element.hidden)visible++;});
    sections.forEach(section=>section.hidden=![...section.querySelectorAll('[data-home-action]')].some(element=>!element.hidden));
    root.querySelector('#hollHomeEmpty').hidden=visible>0;
    root.querySelector('#hollHomeResult').textContent=visible+' '+(visible===1?'acceso disponible':'accesos disponibles');
    clear.hidden=!query.value;
    buttons.forEach(button=>{button.setAttribute('aria-pressed',String(button.dataset.homeFilter===kind));if(button.dataset.homeFilter!=='all')button.hidden=!catalog.some(item=>item.kind===button.dataset.homeFilter);});
  }
  function reset(){query.value='';kind='all';update();query.focus();}
  query.addEventListener('input',update);
  query.addEventListener('keydown',event=>{
    if(event.key==='Escape'){event.preventDefault();reset();}
    if(event.key==='Enter'){
      event.preventDefault();const results=actions.filter(element=>!element.hidden);
      if(results.length===1)results[0].click();else results[0]?.focus();
    }
  });
  buttons.forEach(button=>button.addEventListener('click',()=>{kind=button.dataset.homeFilter;update();}));
  clear.addEventListener('click',reset);root.querySelector('#hollHomeReset').addEventListener('click',reset);
  document.addEventListener('keydown',event=>{if(event.key==='/'&&!event.ctrlKey&&!event.metaKey&&!event.target.closest('input,textarea,select')&&matchMedia('(max-width:991.98px)').matches){event.preventDefault();query.focus();}});
  update();
})();
