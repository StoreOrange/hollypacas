/* Geometry only: millimetres, shared with editor interaction tests. */
window.ShoeLabelGuides = (() => {
  const right = n => n.left + n.width;
  const bottom = n => n.top + n.height;
  function collisions(moving, others, gap=.5) {
    return others.filter(n => n.id !== moving.id &&
      Math.min(right(moving),right(n)) > Math.max(moving.left,n.left)-gap &&
      Math.min(bottom(moving),bottom(n)) > Math.max(moving.top,n.top)-gap);
  }
  function inspect(moving, nodes, width, height, margin, tolerance) {
    const x=[moving.left,moving.left+moving.width/2,right(moving)];
    const y=[moving.top,moving.top+moving.height/2,bottom(moving)];
    const lines=[], related=new Set();
    const add=(axis,value,label,id)=>{if(!lines.some(l=>l.axis===axis&&Math.abs(l.value-value)<.001))lines.push({axis,value,label});if(id)related.add(id);};
    for(const [value,label] of [[margin,'Margen izquierdo'],[width/2,'Centro de etiqueta'],[width-margin,'Margen derecho']]) {
      const candidates=value===width/2?[x[1]]:x;
      if(candidates.some(p=>Math.abs(p-value)<=tolerance))add('x',value,label);
    }
    for(const [value,label] of [[margin,'Margen superior'],[height/2,'Centro de etiqueta'],[height-margin,'Margen inferior']]) {
      const candidates=value===height/2?[y[1]]:y;
      if(candidates.some(p=>Math.abs(p-value)<=tolerance))add('y',value,label);
    }
    for(const n of nodes){
      if(n.id===moving.id)continue;
      const nx=[n.left,n.left+n.width/2,right(n)],ny=[n.top,n.top+n.height/2,bottom(n)];
      for(let i=0;i<3;i++){
        if(Math.abs(x[i]-nx[i])<=tolerance)add('x',nx[i],`${['Borde izquierdo','Centro','Borde derecho'][i]} · ${n.label}`,n.id);
        if(Math.abs(y[i]-ny[i])<=tolerance)add('y',ny[i],`${['Borde superior','Centro','Borde inferior'][i]} · ${n.label}`,n.id);
      }
      if(moving.id!=='barcode'&&n.id!=='barcode'&&Math.abs(moving.y-n.y)<=tolerance)add('y',n.y,`Base de texto · ${n.label}`,n.id);
    }
    const gaps={};
    const consider=(direction,gap,from,to,n)=>{if(gap>=0&&(!gaps[direction]||gap<gaps[direction].gap))gaps[direction]={direction,gap,from,to,label:n.label,id:n.id};};
    for(const n of nodes){
      if(n.id===moving.id)continue;
      const overlapX=Math.min(right(moving),right(n))-Math.max(moving.left,n.left);
      const overlapY=Math.min(bottom(moving),bottom(n))-Math.max(moving.top,n.top);
      if(overlapX>0){
        const cx=(Math.max(moving.left,n.left)+Math.min(right(moving),right(n)))/2;
        consider('arriba',moving.top-bottom(n),[cx,bottom(n)],[cx,moving.top],n);
        consider('abajo',n.top-bottom(moving),[cx,bottom(moving)],[cx,n.top],n);
      }
      if(overlapY>0){
        const cy=(Math.max(moving.top,n.top)+Math.min(bottom(moving),bottom(n)))/2;
        consider('izquierda',moving.left-right(n),[right(n),cy],[moving.left,cy],n);
        consider('derecha',n.left-right(moving),[right(moving),cy],[n.left,cy],n);
      }
    }
    return {lines,related:[...related],gaps:Object.values(gaps),collisions:collisions(moving,nodes)};
  }
  return {inspect,collisions};
})();
