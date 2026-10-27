/* Local PDF rendering and letter printing; lazy-loaded Mozilla PDF.js. */
window.AccountingVoucherPreview = function(element) {
  'use strict';
  const pages=element.querySelector('#voucherPdfPages');
  const link=element.querySelector('#voucherPdfOpen');
  const button=element.querySelector('#voucherPdfPrint');
  const status=element.querySelector('#voucherPrintMessage');
  const vendor='/static/vendor/pdfjs-6.4.299/';
  let generation=0,task=null,renderTask=null,url=null,ready=false;
  async function render(){
    const token=++generation;ready=false;button.disabled=true;pages.replaceChildren();
    status.textContent='Cargando comprobante…';
    try{
      const pdfjs=await import(vendor+'pdf.min.mjs');
      if(token!==generation)return;
      pdfjs.GlobalWorkerOptions.workerSrc=vendor+'pdf.worker.min.mjs';
      task=pdfjs.getDocument({url,standardFontDataUrl:vendor+'standard_fonts/',useWasm:false,isEvalSupported:false});
      const pdf=await task.promise;
      for(let number=1;number<=pdf.numPages;number++){
        if(token!==generation)return;
        status.textContent='Preparando página '+number+' de '+pdf.numPages+'…';
        const page=await pdf.getPage(number);
        if(token!==generation)return;
        const viewport=page.getViewport({scale:2});
        const sheet=document.createElement('div');sheet.className='voucher-pdf-sheet';
        const canvas=document.createElement('canvas');canvas.width=Math.ceil(viewport.width);canvas.height=Math.ceil(viewport.height);
        canvas.setAttribute('role','img');canvas.setAttribute('aria-label','Página '+number+' del comprobante contable');
        sheet.append(canvas);pages.append(sheet);
        renderTask=page.render({canvasContext:canvas.getContext('2d'),viewport});
        await renderTask.promise;renderTask=null;page.cleanup();
      }
      if(token!==generation)return;
      ready=true;button.disabled=false;
      status.textContent=pdf.numPages+' '+(pdf.numPages===1?'página':'páginas')+' · Papel carta.';
    }catch(error){
      if(token!==generation)return;
      pages.replaceChildren();status.textContent='No se pudo cargar el visor. Usa Abrir PDF para ver o imprimir el comprobante.';
    }
  }
  function open(id){
    if(!/^\d+$/.test(String(id))||Number(id)<1)return;
    url='/accounting/entries/'+id+'/pdf';link.href=url;
    if(element.classList.contains('show'))render();
    else bootstrap.Modal.getOrCreateInstance(element).show();
  }
  element.addEventListener('shown.bs.modal',render);
  element.addEventListener('hidden.bs.modal',()=>{
    generation++;ready=false;button.disabled=true;
    if(renderTask){renderTask.cancel();renderTask=null;}
    if(task){task.destroy().catch(()=>{});task=null;}
    pages.replaceChildren();document.getElementById('voucherPrintFrame')?.remove();
  });
  button.addEventListener('click',async()=>{
    if(!ready)return;
    button.disabled=true;
    try{
      // Print the same rendered PDF pages on letter sheets, including all pages.
      // No dependence on a browser's built-in PDF plugin or an external service.
      document.getElementById('voucherPrintFrame')?.remove();
      const frame=document.createElement('iframe');frame.id='voucherPrintFrame';frame.title='Impresión del comprobante';
      frame.style.cssText='position:fixed;width:1px;height:1px;left:-10000px;top:0;border:0';document.body.append(frame);
      const images=[...pages.querySelectorAll('canvas')].map(canvas=>canvas.toDataURL('image/png'));
      const doc=frame.contentDocument;
      doc.open();doc.write('<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Comprobante contable</title><style>@page{size:letter;margin:0}html,body{margin:0;padding:0}.sheet{width:8.5in;height:11in;break-after:page;page-break-after:always}.sheet:last-child{break-after:auto;page-break-after:auto}img{display:block;width:100%;height:100%}</style></head><body>'+images.map(src=>'<div class="sheet"><img alt="Página del comprobante" src="'+src+'"></div>').join('')+'</body></html>');doc.close();
      await Promise.all([...doc.images].map(img=>img.decode()));
      frame.contentWindow.focus();frame.contentWindow.print();
      status.textContent='Comprobante preparado para imprimir en papel carta.';
    }catch(error){status.textContent='Usa Abrir PDF y selecciona Imprimir en el visor del navegador.';link.focus();}
    finally{button.disabled=!ready;}
  });
  return {open};
};
