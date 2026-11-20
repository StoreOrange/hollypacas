(()=>{'use strict';const root=document.getElementById('native-pos');if(!root)return;
const toggle=document.getElementById('native-pos-enabled'),badge=document.getElementById('native-pos-status'),last=document.getElementById('native-pos-last'),retry=document.getElementById('native-pos-retry'),clock=document.getElementById('native-pos-time');
const worker=root.dataset.worker==='1';
const key=`holl-native-pos:${root.dataset.user}:${root.dataset.bodega}`;
let printTimeout=null,ownsQueue=!worker,backupWindow=null,backupClaim=null,ackInFlight=null,backupStarting=false;
function openConsole(){const popup=window.open('/sales/automatico-pos/'+station+'/console','holl-pos-'+station,'popup,width=620,height=420');if(!popup){status('Permite ventanas emergentes y pulsa Abrir impresión',true);return false;}return true;}
document.getElementById('native-pos-console')?.addEventListener('click',openConsole);
let station=null,working=false,frame=null,active=null,data=null,registered=false,changing=false,version=0,connectionError=null;const finishedJobs=new Set(),deadlines=new Map();
const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let recoveryForced=false;
const recovery=document.createElement('section');recovery.className='pos-recovery';recovery.hidden=true;recovery.setAttribute('aria-label','Respaldo de impresión');
recovery.innerHTML='<h2>Comprobantes pendientes de impresión</h2><p>Revisa la impresora y la cola de Windows. El respaldo incluye los ejemplares de cada factura pendiente, sin volver a facturar.</p><ul></ul><p class="pos-recovery-message" role="status"></p><button type="button" data-backup>Imprimir todos los pendientes</button><button type="button" class="pos-recovery-close" data-close>Cerrar</button><small>Si un comprobante ya salió, no lo vuelvas a imprimir. Firefox confirma el ciclo del navegador, no la salida física del papel.</small>';
(document.querySelector('#layout main')||document.body).prepend(recovery);
const pendingButton=document.createElement('button');pendingButton.type='button';pendingButton.className='btn btn-sm btn-outline-primary';pendingButton.textContent='Ver pendientes';pendingButton.hidden=true;root.appendChild(pendingButton);
pendingButton.addEventListener('click',()=>{recoveryForced=true;renderRecovery();recovery.scrollIntoView({block:'center',behavior:'smooth'});});
recovery.querySelector('[data-close]').addEventListener('click',()=>{recoveryForced=false;recovery.hidden=true;});
function renderRecovery(){if(!data)return;const tasks=data.tasks.filter(task=>task.job_id&&['QUEUED','DISPATCHED','UNKNOWN','FAILED'].includes(task.print_status));pendingButton.hidden=!tasks.length;pendingButton.textContent=`Ver pendientes (${tasks.length})`;const failed=tasks.some(task=>['UNKNOWN','FAILED'].includes(task.print_status)||task.retry_allowed);recovery.hidden=!tasks.length||!(failed||recoveryForced);const list=recovery.querySelector('ul');const signature=JSON.stringify(tasks.map(task=>[task.job_id,task.print_status,task.retry_allowed,task.invoice_number,task.copies]));if(recovery.dataset.signature!==signature){const choices=new Map([...list.querySelectorAll('[data-backup-job]')].map(input=>[Number(input.dataset.backupJob),input.checked]));list.innerHTML=tasks.map(task=>`<li><label><input type="checkbox" data-backup-job="${task.job_id}" ${choices.get(task.job_id)===false?'':'checked'}> <strong>${escape(task.invoice_number||task.preventa)}</strong> · ${task.copies||3} ejemplares · ${task.print_status==='QUEUED'?'En espera':task.print_status==='DISPATCHED'&&!task.retry_allowed?'Imprimiendo':'Necesita revisión'}</label></li>`).join('');recovery.dataset.signature=signature;}recovery.querySelector('[data-backup]').disabled=Boolean(tasks.some(task=>task.print_status==='DISPATCHED'&&!task.retry_allowed)||localStorage.getItem(key+':ack')||backupClaim||backupStarting);}
recovery.querySelector('[data-backup]').addEventListener('click',async()=>{
 const ids=[...recovery.querySelectorAll('[data-backup-job]:checked')].map(input=>Number(input.dataset.backupJob));
 if(!ids.length){recovery.querySelector('[role="status"]').textContent='Selecciona al menos una factura.';return;}
 if(!confirm('¿Revisaste la impresora y confirmas que estos comprobantes no salieron? Se imprimirán las mismas facturas, sin generar ventas nuevas.'))return;
 backupWindow=window.open('','holl-pos-backup-'+station,'popup,width=760,height=800');
 if(!backupWindow){recovery.querySelector('[role="status"]').textContent='Permite ventanas emergentes para abrir el respaldo.';return;}
 backupWindow.document.body.textContent='Preparando comprobantes existentes…';
 const button=recovery.querySelector('[data-backup]');button.disabled=true;backupStarting=true;
 try{backupClaim=await api(station+'/backup/claim',{confirmed:true,job_ids:ids});clearTimeout(printTimeout);frame?.remove();frame=null;active=null;backupWindow.location.replace(backupClaim.url);armPrintTimeout();}
 catch(error){backupWindow.close();backupClaim=null;recovery.querySelector('[role="status"]').textContent=error.message;button.disabled=false;}finally{backupStarting=false;}
});
function status(text,error=false){badge.hidden=!text;badge.textContent=text;badge.className='badge '+(error?'text-bg-warning':'text-bg-success');badge.title=text;}
async function api(path,payload={}){
 const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),8000);
 try{const response=await fetch('/sales/automatico-pos/'+path,{method:'POST',credentials:'same-origin',cache:'no-store',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload),redirect:'error',signal:controller.signal});if(!response.ok){let text='Sin conexión';try{text=(await response.json()).detail||text}catch{}const error=Error(text);error.status=response.status;throw error;}return await response.json();}
 catch(error){if(error.name==='AbortError'||error.name==='TypeError')throw Error('Reconectando…');throw error;}
 finally{clearTimeout(timeout);}
}

function busy(){if(worker){try{const mark=JSON.parse(localStorage.getItem(key+':busy')||'null');return Boolean(mark?.busy&&Date.now()-mark.at<6000);}catch{return true;}}return Boolean(document.querySelector('#items-table input[name="item_producto_id"]')||document.getElementById('preventa-id')?.value||document.querySelector('.modal.show'));}
function render(state){
 for(const id of state.completed_preventa_ids||[])window.dispatchEvent(new CustomEvent('holl-preventa-facturada',{detail:{id}}));
 for(const task of state.tasks){if(task.invoice_id)window.dispatchEvent(new CustomEvent('holl-preventa-facturada',{detail:{id:task.preventa_id}}));if(task.print_status==='SENT')finishedJobs.add(task.job_id);}
 state={...state,tasks:state.tasks.filter(task=>task.print_status!=='SENT'&&!finishedJobs.has(task.job_id))};
 for(const task of state.tasks){if(task.status==='WAITING'){const end=Date.now()+task.remaining*1000;deadlines.set(task.id,Math.min(deadlines.get(task.id)??end,end));}}
 data=state;toggle.checked=state.enabled;draw();const invoice=[...state.tasks].filter(task=>task.invoice_id).sort((a,b)=>b.invoice_id-a.invoice_id)[0];last.hidden=!invoice;if(!invoice){last.textContent='';last.removeAttribute('title');}if(invoice){last.innerHTML=`<a href="/sales/${invoice.invoice_id}/pdf" target="_blank" rel="noopener">${escape(invoice.invoice_number)}</a>`;last.title=`Efectivo C$ ${invoice.total}`;}
 const review=state.tasks.find(task=>task.print_status==='UNKNOWN'||(task.print_status==='DISPATCHED'&&task.retry_allowed));retry.hidden=!review;retry.dataset.job=review?.job_id||'';}
function draw(){renderRecovery();if(connectionError){status(connectionError,true);return;}if(!data)return;const waiting=data.tasks.find(task=>task.status==='WAITING');const left=waiting?Math.max(0,Math.ceil(((deadlines.get(waiting.id)??Date.now())-Date.now())/1000)):(data.delay_seconds||15);if(clock)clock.textContent=left+' s';if(localStorage.getItem(key+':ack')){status('Confirmación pendiente');return;}const problem=data.tasks.find(task=>task.status==='ERROR');const printing=data.tasks.find(task=>task.print_status==='DISPATCHED'&&!task.retry_allowed);const unknown=data.tasks.find(task=>['UNKNOWN','FAILED'].includes(task.print_status)||task.retry_allowed);const task=data.tasks.find(task=>task.status==='WAITING');if(unknown){status('Revisar Epson',true);badge.title=unknown.print_message||'Revisa la cola antes de reimprimir';}else if(printing){status('Imprimiendo…');}else if(data.enabled&&busy()){status('En pausa');badge.title='Termina o limpia la venta actual';}else if(data.enabled&&task){status(left>0?task.preventa:'Procesando…');}else if(problem){status('Revisar preventa',true);badge.title=problem.message;}else{status(data.enabled?'Activo':'Apagado');if(!data.enabled)badge.className='badge text-bg-secondary';}}
async function doFlushAck(){const raw=localStorage.getItem(key+':ack');if(!raw)return;let ack;try{ack=JSON.parse(raw);}catch{localStorage.removeItem(key+':ack');throw Error('Revisa los comprobantes pendientes');}let updated=null;try{updated=await api(ack.batch?`${station}/backup/ack`:`${station}/jobs/${ack.job_id}/ack`,ack);}catch(error){if(error.status!==409)throw error;}if(localStorage.getItem(key+':ack')===raw)localStorage.removeItem(key+':ack');if(updated&&ack.status==='SENT')for(const id of ack.job_ids||[ack.job_id]){if(!updated.tasks?.some(task=>task.job_id===id&&task.print_status!=='SENT'))finishedJobs.add(id);}if(active?.claim_id===ack.claim_id){clearTimeout(printTimeout);frame?.remove();frame=null;active=null;}if(updated?.tasks)render(updated);else if(data)render(data);}
function flushAck(){if(!ackInFlight)ackInFlight=doFlushAck().finally(()=>{ackInFlight=null;});return ackInFlight;}


async function printNext(){if(frame||active||!data?.tasks.some(task=>task.print_status==='QUEUED'))return;const result=await api(station+'/claim');if(!result.job)return;active=result.job;frame=document.createElement('iframe');frame.title='Ticket POS';frame.style.cssText='position:fixed;left:-10000px;top:0;width:320px;height:600px;border:0;';frame.src=active.url;document.body.appendChild(frame);armPrintTimeout();}
async function tick(){
 if(!ownsQueue||!station||working||changing)return;
 if(worker)return poll(true);
 // La ventana de caja conserva el bloqueo durante toda su vida. Si falta,
 // Ventas toma el control automáticamente, sin depender de un popup permitido.
 if(navigator.locks)return navigator.locks.request(key,{ifAvailable:true},lock=>poll(Boolean(lock)));
 return poll(true);
}
async function poll(processQueue){if(working||changing)return;working=true;const currentVersion=version;try{
 if(!registered){render(await api('station',{station_id:station}));registered=true;toggle.disabled=false;}
 if(!worker)localStorage.setItem(key+':busy',JSON.stringify({busy:busy(),at:Date.now()}));
 if(processQueue){localStorage.setItem(key+':heartbeat',String(Date.now()));await flushAck();}
 const result=await api(station+(processQueue?'/tick':'/state'),{busy:busy()||Boolean(frame)});if(currentVersion!==version)return;connectionError=null;render(result);if(processQueue)await printNext();
 }catch(error){connectionError=error.message||'Reconectando…';status(connectionError,true);}finally{working=false;}}

async function recordAck(message){try{if(message.batch&&backupClaim?.claim_id===message.claim_id)backupClaim=null;localStorage.setItem(key+':ack',JSON.stringify(message));if(data)render(data);await flushAck();tick();}catch(error){status('Confirmación pendiente',true);}}

function armPrintTimeout(){clearTimeout(printTimeout);printTimeout=setTimeout(()=>{if(backupClaim)recordAck({batch:true,job_ids:backupClaim.job_ids,claim_id:backupClaim.claim_id,status:'UNKNOWN'});else if(active)recordAck({job_id:active.id,claim_id:active.claim_id,status:'UNKNOWN'});},40000);}
function progress(payload){armPrintTimeout();api(payload.batch?`${station}/backup/ack`:`${station}/jobs/${payload.job_id}/ack`,{...payload,status:'PROGRESS'}).catch(()=>{});}
window.addEventListener('message',event=>{if(event.origin===location.origin&&event.source===backupWindow&&event.data?.type==='holl-pos-backup'&&event.data.claim_id===backupClaim?.claim_id&&['SENT','UNKNOWN','PROGRESS'].includes(event.data.status)){const payload={batch:true,job_ids:backupClaim.job_ids,claim_id:backupClaim.claim_id,status:event.data.status};if(event.data.status==='PROGRESS')progress(payload);else recordAck(payload);return;}if(!frame||event.source!==frame.contentWindow||event.origin!==location.origin||event.data?.type!=='holl-native-print')return;const message=event.data;if(message.claim_id!==active?.claim_id||message.job_id!==active?.id||!['SENT','UNKNOWN','PROGRESS'].includes(message.status))return;if(message.status==='PROGRESS')progress(message);else recordAck(message);});
toggle.addEventListener('change',async()=>{const desired=toggle.checked;if(desired&&!worker)openConsole();version++;changing=true;toggle.disabled=true;try{const result=await api(station+'/enable',{enabled:desired});connectionError=null;render(result);}catch(error){toggle.checked=!desired;status(error.message,true);}finally{changing=false;toggle.disabled=false;tick();}});
retry.addEventListener('click',()=>{recoveryForced=true;renderRecovery();recovery.scrollIntoView({block:'center',behavior:'smooth'});});

try{station=root.dataset.station||localStorage.getItem(key);if(!/^[a-f0-9]{32}$/.test(station||'')){station=crypto.randomUUID().replaceAll('-','');localStorage.setItem(key,station);}window.addEventListener('pagehide',()=>{if(!worker)localStorage.removeItem(key+':busy');});setInterval(tick,2000);setInterval(draw,1000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)tick();});window.addEventListener('online',tick);window.addEventListener('focus',tick);window.addEventListener('pageshow',tick);window.addEventListener('holl-preventa-recibida',tick);status('Conectando…');if(worker){if(!navigator.locks){status('Este navegador no permite controlar la cola de forma segura',true);return;}const acquire=()=>navigator.locks.request(key,{ifAvailable:true},async lock=>{if(!lock){status('La cola ya está abierta en otra ventana',true);setTimeout(acquire,1000);return;}ownsQueue=true;tick();await new Promise(()=>{});});acquire();}else tick();}catch(error){status(error.message,true);}
})();
