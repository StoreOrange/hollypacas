(() => {
  const $ = id => document.getElementById(id);
  const clone = value => JSON.parse(JSON.stringify(value));
  let design = JSON.parse($('label-design-data').textContent);
  const defaults = JSON.parse($('label-default-data').textContent);
  const svg = $('label-board'), form = $('design-form');
  const NS = 'http://www.w3.org/2000/svg';
  let selected = 'name', geometry = null, drag = null, raf = 0, timer;
  let revision = 0, controller = null, busy = false, pdfUrl = null;
  const history = [];
  const specs = [
    ['width','Ancho (mm)',30,150,.1],['height','Alto (mm)',20,150,.1],['margin','Margen (mm)',2,10,.1],
    ['barcode_height','Alto de barras (mm)',5,100,.1],['barcode_width','Grosor (mm)',.25,.6,.01],
    ['barcode_max_width','Ancho máx. barras (mm)',20,140,.1],['offset_x','Desplazamiento X',-10,10,.1],['offset_y','Desplazamiento Y',-10,10,.1]
  ];
  const anchor = f => f.x ?? ({left:design.margin,center:design.width/2,right:design.width-design.margin}[f.align]);
  const node = () => geometry?.nodes.find(n=>n.id===selected);
  function checkpoint(){history.push(clone(design));if(history.length>60)history.shift();$('undo').disabled=false;}
  function dirty(){revision++;controller?.abort();clearTimeout(timer);$('status').textContent='Cambios sin guardar.';$('pdf-panel').hidden=true;}
  function schedule(){clearTimeout(timer);timer=setTimeout(refresh,180);}
  function element(tag,attrs={},text){const el=document.createElementNS(NS,tag);for(const [key,v] of Object.entries(attrs))el.setAttribute(key,v);if(text!==undefined)el.textContent=text;return el;}
  function inspector(){
    const bar=selected==='barcode', f=design.fields[selected];
    $('element').value=selected;
    $('element-x').value=Number((bar?(design.barcode_x??design.width/2):anchor(f)).toFixed(2));
    $('element-y').value=Number((bar?design.barcode_top:f.top).toFixed(2));
    $('text-controls').hidden=bar;
    if(!bar){$('element-size').value=f.size;$('element-align').value=f.align;$('element-bold').checked=f.bold;$('element-visible').checked=f.visible;}
    svg.querySelectorAll('.label-node').forEach(g=>g.classList.toggle('selected',g.dataset.id===selected));
    drawGuides();
  }
  function drawGuides(moving=node()){
    svg.querySelector('.label-guides')?.remove();
    svg.querySelectorAll('.label-node').forEach(g=>g.classList.remove('related','colliding'));
    if(!moving||!geometry)return;
    const pixels=svg.getBoundingClientRect().width/geometry.width || 10;
    const info=ShoeLabelGuides.inspect(moving,geometry.nodes,geometry.width,geometry.height,geometry.margin,Math.min(.4,5/pixels));
    const collisionIds=new Set(info.collisions.map(n=>n.id));
    if(collisionIds.size)collisionIds.add(moving.id);
    svg.querySelectorAll('.label-node').forEach(g=>{g.classList.toggle('colliding',collisionIds.has(g.dataset.id));g.classList.toggle('related',info.related.includes(g.dataset.id));});
    if(!$('show-guides').checked){$('alignment-status').textContent=info.collisions.length?'Esta posición se superpone: mueva el elemento antes de soltar.':'Guías ocultas. La protección contra choques sigue activa.';return;}
    const layer=element('g',{'class':'label-guides','aria-hidden':'true'});
    const stroke=1/pixels;
    for(const l of info.lines){
      layer.appendChild(element('line',{x1:l.axis==='x'?l.value:0,y1:l.axis==='y'?l.value:0,x2:l.axis==='x'?l.value:geometry.width,y2:l.axis==='y'?l.value:geometry.height,stroke:'#2563eb','stroke-width':stroke,'stroke-dasharray':`${4/pixels} ${3/pixels}`}));
    }
    for(const gap of info.gaps){
      const [x1,y1]=gap.from,[x2,y2]=gap.to;
      const color=gap.gap<.5?'#dc2626':'#15803d';
      layer.appendChild(element('line',{x1,y1,x2,y2,stroke:color,'stroke-width':stroke}));
      const tick=3/pixels;
      for(const [x,y] of [gap.from,gap.to])layer.appendChild(element('line',{x1:x-(x1===x2?tick:0),x2:x+(x1===x2?tick:0),y1:y-(y1===y2?tick:0),y2:y+(y1===y2?tick:0),stroke:color,'stroke-width':stroke}));
      const label=`${gap.gap.toFixed(1)} mm`;
      const tx=Math.max(4,Math.min(geometry.width-4,(x1+x2)/2+(x1===x2?22/pixels:0)));
      const ty=Math.max(1,Math.min(geometry.height-1,(y1+y2)/2-(y1===y2?5/pixels:0)));
      layer.appendChild(element('text',{x:tx,y:ty,'text-anchor':'middle','dominant-baseline':'middle','font-family':'Arial','font-size':11/pixels,fill:color,stroke:'white','stroke-width':3/pixels,'paint-order':'stroke'},label));
    }
    svg.appendChild(layer);
    $('alignment-status').textContent=info.collisions.length?`Choque con ${info.collisions.map(n=>n.label).join(', ')}. Deje al menos 0.5 mm; al soltar se conservará la posición anterior.`:[...new Set(info.lines.map(l=>l.label)),...info.gaps.map(g=>`${g.label}: ${g.gap.toFixed(1)} mm ${g.direction}`)].join(' · ')||'Movimiento libre: acerque bordes o centros para ver guías de alineación.';
  }
  function populate(){
    $('element').replaceChildren();
    for(const [key,f] of Object.entries(design.fields))$('element').add(new Option(f.label,key));
    $('element').add(new Option('Código de barras','barcode'));
    $('dimensions').innerHTML='';$('advanced').innerHTML='';
    specs.forEach(([key,label,min,max,step],i)=>{
      const div=document.createElement('div');div.className='col-6';
      div.innerHTML=`<label class="form-label small" for="setting-${key}">${label}</label><input id="setting-${key}" data-setting="${key}" class="form-control" type="number" required min="${min}" max="${max}" step="${step}">`;
      div.querySelector('input').value=design[key];$(i<3?'dimensions':'advanced').appendChild(div);
    });
    $('font').value=design.font;$('rotation').value=design.rotation;inspector();
  }
  function resize(){if(!geometry)return;const available=$('board-container').clientWidth-48;svg.style.width=Math.max(200,available)*Number($('zoom').value)+'px';drawGuides();}
  function draw(){
    if(!geometry)return;
    svg.replaceChildren();svg.setAttribute('viewBox',`0 0 ${geometry.width} ${geometry.height}`);
    svg.appendChild(element('rect',{x:geometry.margin,y:geometry.margin,width:geometry.width-geometry.margin*2,height:geometry.height-geometry.margin*2,fill:'none',stroke:'#94a3b8','stroke-width':.12,'stroke-dasharray':'.8 .6','pointer-events':'none'}));
    for(const n of geometry.nodes){
      const g=element('g',{'class':'label-node',tabindex:0,role:'button','aria-label':`Mover ${n.label}`,'data-id':n.id});
      g.appendChild(element('title',{},`${n.label}: arrastre o use las flechas`));
      if(n.id==='barcode'){
        const bars=element('g',{'class':'bars',fill:'#000'});
        for(const [x,y,w,h] of n.bars)bars.appendChild(element('rect',{x:n.left+x,y:n.top+n.height-h-y,width:w,height:h}));
        g.appendChild(bars);
        if(!n.bars.length){g.appendChild(element('rect',{x:n.left,y:n.top,width:n.width,height:n.height,fill:'#fff1f2',stroke:'#dc2626','stroke-width':.15}));g.appendChild(element('text',{x:n.x,y:n.top+n.height/2,'text-anchor':'middle','font-size':1.5,fill:'#b91c1c'},'Código demasiado largo para este ancho'));}
      }else{
        const family=n.font.startsWith('Times')?'Times New Roman':n.font.startsWith('Courier')?'Courier New':'Arial, Helvetica, sans-serif';
        const attrs={x:n.left,y:n.y,'font-family':family,'font-size':n.size*25.4/72,'font-weight':n.bold?'bold':'normal',fill:'#000'};
        if(n.width>0){attrs.textLength=n.width;attrs.lengthAdjust='spacingAndGlyphs';}
        g.appendChild(element('text',attrs,n.text));
      }
      g.appendChild(element('rect',{x:n.left-.6,y:n.top-.6,width:Math.max(n.width,2)+1.2,height:n.height+1.2,rx:.4,'class':'node-hit'}));
      svg.appendChild(g);
    }
    resize();inspector();$('geometry-errors').textContent=geometry.errors.join(' ');
  }
  async function refresh(){
    if(busy||drag)return;
    if(!form.checkValidity()){$('geometry-errors').textContent='Revise los valores numéricos del editor.';return;}
    controller?.abort();controller=new AbortController();const current=revision;
    try{
      const response=await fetch('/data/etiquetas-zapatos/layout',{method:'POST',headers:{'Content-Type':'application/json','X-Requested-With':'fetch'},body:JSON.stringify(design),signal:controller.signal});
      if(response.redirected)throw new Error('Su sesión expiró. Inicie sesión nuevamente.');
      const data=await response.json();if(!response.ok)throw new Error(data.detail||'No se pudo actualizar el lienzo');
      if(current!==revision||drag)return;geometry=data;draw();
    }catch(err){if(err.name!=='AbortError'&&current===revision)$('geometry-errors').textContent=err.message;}
  }
  function coords(event){const p=svg.createSVGPoint();p.x=event.clientX;p.y=event.clientY;return p.matrixTransform(svg.getScreenCTM().inverse());}
  function bounded(n,dx,dy){
    return {dx:Math.max(design.margin-n.left,Math.min(design.width-design.margin-n.left-n.width,dx)),dy:Math.max(design.margin-n.top,Math.min(design.height-design.margin-n.top-n.height,dy))};
  }
  function commitMove(n,dx,dy){
    if(n.id==='barcode'){design.barcode_x=n.x-design.offset_x+dx;design.barcode_top=n.y-design.offset_y+dy;}
    else {design.fields[n.id].x=n.x-design.offset_x+dx;design.fields[n.id].top=n.y-design.offset_y+dy;}
  }
  svg.addEventListener('focusin',event=>{const g=event.target.closest('.label-node');if(g && g.dataset.id!==selected){selected=g.dataset.id;inspector();}});
  svg.addEventListener('pointerdown',event=>{
    if(busy||event.button!==0)return;const g=event.target.closest('.label-node');if(!g)return;
    selected=g.dataset.id;inspector();const n=node();if(!n)return;
    event.preventDefault();clearTimeout(timer);controller?.abort();
    g.focus({preventScroll:true});svg.setPointerCapture(event.pointerId);
    drag={g,n,start:coords(event),dx:0,dy:0,pointer:event.pointerId};g.classList.add('dragging');
  });
  svg.addEventListener('pointermove',event=>{
    if(!drag||event.pointerId!==drag.pointer)return;const p=coords(event);
    Object.assign(drag,bounded(drag.n,p.x-drag.start.x,p.y-drag.start.y));
    if(!raf)raf=requestAnimationFrame(()=>{raf=0;if(drag){drag.g.setAttribute('transform',`translate(${drag.dx} ${drag.dy})`);$('element-x').value=(drag.n.x-design.offset_x+drag.dx).toFixed(2);$('element-y').value=(drag.n.y-design.offset_y+drag.dy).toFixed(2);drawGuides({...drag.n,left:drag.n.left+drag.dx,top:drag.n.top+drag.dy,x:drag.n.x+drag.dx,y:drag.n.y+drag.dy});}});
  });
  function endDrag(event,cancel=false){
    if(!drag||event.pointerId!==drag.pointer)return;const d=drag;drag=null;if(raf){cancelAnimationFrame(raf);raf=0;}
    d.g.classList.remove('dragging');
    if(cancel){d.g.removeAttribute('transform');inspector();return;}
    const moved={...d.n,left:d.n.left+d.dx,top:d.n.top+d.dy,x:d.n.x+d.dx,y:d.n.y+d.dy};
    if(ShoeLabelGuides.collisions(moved,geometry.nodes).length){d.g.removeAttribute('transform');inspector();$('status').textContent='Movimiento descartado por superposición. Se conservó la posición anterior.';return;}
    if(Math.abs(d.dx)+Math.abs(d.dy)>.001){checkpoint();commitMove(d.n,d.dx,d.dy);dirty();d.g.setAttribute('transform',`translate(${d.dx} ${d.dy})`);d.n.left+=d.dx;d.n.top+=d.dy;d.n.x+=d.dx;d.n.y+=d.dy;}
    inspector();schedule();
  }
  svg.addEventListener('pointerup',e=>endDrag(e));svg.addEventListener('pointercancel',e=>endDrag(e,true));svg.addEventListener('lostpointercapture',e=>{if(drag)endDrag(e,true);});
  svg.addEventListener('keydown',event=>{
    const directions={ArrowLeft:[-1,0],ArrowRight:[1,0],ArrowUp:[0,-1],ArrowDown:[0,1]};
    if(busy||!directions[event.key])return;event.preventDefault();const n=node();if(!n)return;
    const step=event.shiftKey?1:.1;const [x,y]=directions[event.key];const {dx,dy}=bounded(n,x*step,y*step);
    if(ShoeLabelGuides.collisions({...n,left:n.left+dx,top:n.top+dy,x:n.x+dx,y:n.y+dy},geometry.nodes).length){$('status').textContent='No se movió: deje 0.5 mm de separación con los otros elementos.';return;}
    checkpoint();commitMove(n,dx,dy);n.left+=dx;n.top+=dy;n.x+=dx;n.y+=dy;dirty();draw();svg.querySelector(`[data-id="${selected}"]`)?.focus();schedule();
  });
  $('element').addEventListener('change',()=>{selected=$('element').value;inspector();});
  form.addEventListener('submit',e=>e.preventDefault());
  form.addEventListener('change',event=>{
    const el=event.target;if(el.id==='element')return;if(!el.checkValidity())return;
    checkpoint();const bar=selected==='barcode',f=design.fields[selected];
    if(el.dataset.setting)design[el.dataset.setting]=Number(el.value);
    else if(el.id==='font')design.font=el.value;
    else if(el.id==='rotation')design.rotation=Number(el.value);
    else if(el.id==='element-x'){if(bar)design.barcode_x=Number(el.value);else f.x=Number(el.value);}
    else if(el.id==='element-y'){if(bar)design.barcode_top=Number(el.value);else f.top=Number(el.value);}
    else if(el.id==='element-size')f.size=Number(el.value);
    else if(el.id==='element-align')f.align=el.value;
    else if(el.id==='element-bold')f.bold=el.checked;
    else if(el.id==='element-visible')f.visible=el.checked;
    dirty();refresh();
  });
  $('center-element').addEventListener('click',()=>{checkpoint();if(selected==='barcode')design.barcode_x=design.width/2;else Object.assign(design.fields[selected],{x:design.width/2,align:'center'});dirty();inspector();refresh();});
  $('undo').addEventListener('click',()=>{if(!history.length||busy)return;design=history.pop();dirty();populate();refresh();$('undo').disabled=!history.length;});
  $('reset').addEventListener('click',()=>{checkpoint();design=clone(defaults);dirty();populate();refresh();});
  $('show-guides').addEventListener('change',()=>drawGuides());
  $('zoom').addEventListener('change',resize);new ResizeObserver(resize).observe($('board-container'));
  async function action(save){
    if(busy||drag||!form.reportValidity())return;clearTimeout(timer);controller?.abort();busy=true;
    ++revision;
    document.querySelectorAll('main button,main input,main select').forEach(el=>el.disabled=true);
    $('status').textContent=save?'Guardando diseño…':'Generando PDF…';
    try{
      const response=await fetch('/data/etiquetas-zapatos/'+(save?'save':'preview'),{method:'POST',headers:{'Content-Type':'application/json','X-Requested-With':'fetch'},body:JSON.stringify(design)});
      if(response.redirected)throw new Error('Su sesión expiró. Inicie sesión nuevamente.');
      if(!response.ok){const data=await response.json();throw new Error(data.detail||'No se pudo completar la operación');}
      if(save){design=(await response.json()).design;$('status').textContent='Diseño guardado. Estas posiciones se usarán al imprimir.';}
      else{if(pdfUrl)URL.revokeObjectURL(pdfUrl);pdfUrl=URL.createObjectURL(await response.blob());$('preview-frame').src=pdfUrl;$('pdf-panel').hidden=false;$('status').textContent='PDF generado con las posiciones del lienzo. Guarde para aplicarlas.';}
    }catch(err){$('status').textContent=err.message;}
    finally{busy=false;document.querySelectorAll('main button,main input,main select').forEach(el=>el.disabled=false);$('undo').disabled=!history.length;refresh();}
  }
  $('save-design').addEventListener('click',()=>action(true));$('preview').addEventListener('click',()=>action(false));$('close-pdf').addEventListener('click',()=>{$('pdf-panel').hidden=true;});
  populate();refresh();$('status').textContent='Arrastre un elemento del lienzo. Pulse Guardar diseño para aplicar los cambios.';
})();
