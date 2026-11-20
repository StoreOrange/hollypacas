/* One browser/spooler job per receipt: Epson cuts at the end of each job. */
window.hollPrintReceipts=(receipts,report)=>{
 const schedule=callback=>{const channel=new MessageChannel();channel.port1.onmessage=()=>{channel.port1.close();channel.port2.close();callback();};channel.port2.postMessage(null);};
 let index=0,finished=false,inPrint=false,returned=false,after=false,advancing=false,timer;
 const done=status=>{if(finished)return;finished=true;clearTimeout(timer);window.removeEventListener('afterprint',onAfter);report(status);};
 const complete=()=>{if(finished||!inPrint||!returned||!after||advancing)return;advancing=true;clearTimeout(timer);inPrint=false;report('PROGRESS');schedule(()=>{index++;if(index===receipts.length)done('SENT');else next();});};
 const onAfter=()=>{if(inPrint){after=true;complete();}};
 window.addEventListener('afterprint',onAfter);
 const next=()=>{
  if(finished)return;
  receipts.forEach((receipt,i)=>receipt.style.setProperty('display',i===index?'block':'none','important'));
  const receipt=receipts[index],style=document.getElementById('pos-sequence-paper')||document.head.appendChild(Object.assign(document.createElement('style'),{id:'pos-sequence-paper'}));
  style.textContent=`@page{size:80mm ${Math.ceil(receipt.getBoundingClientRect().height*25.4/96+3)}mm;margin:.5mm 2mm}`;
  report('PROGRESS');returned=false;after=false;advancing=false;inPrint=true;
  timer=setTimeout(()=>done('UNKNOWN'),35000);
  schedule(()=>{if(finished)return;const start=Date.now();try{window.print();returned=true;if(Date.now()-start>600)after=true;complete();}catch(error){done('UNKNOWN');}});
 };
 if(!receipts.length){done('UNKNOWN');return;}next();
};
