"""Agente saliente HTTPS para Windows. No abre puertos ni recibe comandos arbitrarios."""
import base64,ctypes,hashlib,json,logging,os,subprocess,sys,tempfile,time
from pathlib import Path
from urllib.request import Request,build_opener,HTTPRedirectHandler
from urllib.error import HTTPError
from urllib.parse import urlsplit


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


def default_printer():
    if sys.platform!='win32':raise RuntimeError('Este agente requiere Windows')
    length=ctypes.c_ulong(0)
    spooler=ctypes.WinDLL('winspool.drv')
    spooler.GetDefaultPrinterW(None,ctypes.byref(length))
    if not length.value:raise RuntimeError('Configura la Epson térmica como impresora predeterminada de Windows')
    name=ctypes.create_unicode_buffer(length.value)
    if not spooler.GetDefaultPrinterW(name,ctypes.byref(length)):raise RuntimeError('No se pudo consultar la impresora predeterminada')
    if any(word in name.value.lower() for word in ('microsoft print to pdf','xps','onenote','fax')):raise RuntimeError('La impresora predeterminada debe ser la Epson, no una impresora virtual')
    return name.value


def sumatra(config):
    candidates=[config.get('sumatra_path')]+[str(Path(os.getenv(root,''))/'SumatraPDF'/'SumatraPDF.exe') for root in ('LOCALAPPDATA','ProgramFiles','ProgramFiles(x86)')]
    for path in candidates:
        if path and Path(path).is_file():return path
    raise RuntimeError('Instala SumatraPDF desde sumatrapdfreader.org y reinicia el agente')


def atomic_json(path,value):
    temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(value),encoding='utf-8');os.replace(temporary,path)


class Agent:
    def __init__(self,config_path):
        self.path=Path(config_path).resolve();self.config=json.loads(self.path.read_text(encoding='utf-8-sig'))
        parsed=urlsplit(self.config['api_url'])
        if (parsed.scheme!='https' and not (parsed.scheme=='http' and parsed.hostname in ('localhost','127.0.0.1'))) or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('','/'):
            raise RuntimeError('El agente requiere una URL HTTPS de Hollpacas (o localhost para pruebas)')
        self.base=self.config['api_url'].rstrip('/')+'/api/pos-agent/'+self.config['station_id']
        self.state_path=self.path.with_name('trabajos.json')
        self.state=json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.opener=build_opener(NoRedirect())

    def api(self,path,payload):
        req=Request(self.base+path,data=json.dumps(payload).encode(),headers={'Authorization':'Bearer '+self.config['token'],'Content-Type':'application/json'},method='POST')
        with self.opener.open(req,timeout=25) as response:return json.load(response)

    def flush_ack(self):
        for claim,item in list(self.state.items()):
            status=item['status']
            if status=='INFLIGHT':status='UNKNOWN';item['message']='El agente se interrumpió durante el envío. Revisa la cola Windows antes de reimprimir'
            try:
                self.api('/jobs/'+str(item['id'])+'/ack',{'claim_id':claim,'status':status,'message':item['message']})
            except HTTPError as exc:
                if exc.code!=409:raise
                # Un reintento manual reemplazó este intento; no bloquear el agente con su ACK antiguo.
            del self.state[claim];atomic_json(self.state_path,self.state)

    def step(self):
        self.flush_ack()
        try:
            printer=default_printer();exe=sumatra(self.config);ready=True;message='Epson predeterminada disponible para enviar a la cola Windows'
        except RuntimeError as exc:
            printer='';exe=None;ready=False;message=str(exc)
        payload=self.api('/next',{'ready':ready,'printer':printer,'message':message})
        if payload.get('job'):self.print_job(payload['job'],printer,exe)

    def print_job(self,job,printer,exe):
        claim=job['claim_id']
        # Persistir antes del comando. Un corte nunca provoca reenvío automático incierto.
        self.state[claim]={'id':job['id'],'status':'INFLIGHT','message':'Envío iniciado'};atomic_json(self.state_path,self.state)
        try:
            content=base64.b64decode(job['pdf'],validate=True)
            if not content.startswith(b'%PDF-') or hashlib.sha256(content).hexdigest()!=job['sha256']:raise ValueError('El ticket recibido no pasó la verificación')
            copies=int(job['copies'])
            if not 1<=copies<=5:raise ValueError('Cantidad de copias fuera del rango permitido')
            with tempfile.TemporaryDirectory(prefix='HollpacasPOS-') as folder:
                pdf=Path(folder)/'ticket.pdf';pdf.write_bytes(content)
                # Un comando para todas las copias: un fallo posterior puede ser ambiguo.
                result=subprocess.run([exe,'-print-to',printer,'-print-settings',f'noscale,{copies}x','-silent',str(pdf)],timeout=90,check=False,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                if result.returncode!=0:raise RuntimeError('Windows/Sumatra devolvió error '+str(result.returncode)+'. Revisa la cola antes de reimprimir')
            status='SENT';message='Enviado a la cola Windows: '+printer+'. Verifica papel y salida física en la Epson'
        except (ValueError,KeyError) as exc:status='FAILED';message=str(exc)
        except Exception as exc:status='UNKNOWN';message=str(exc)
        self.state[claim].update(status=status,message=message);atomic_json(self.state_path,self.state)
        self.flush_ack()


def main():
    path=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).with_name('hollpacas-pos.json')
    logging.basicConfig(filename=str(path.with_name('agente.log')),level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    # El bloqueo del archivo evita dos agentes imprimiendo con el mismo perfil.
    import msvcrt
    guard=path.with_name('agente.lock').open('a+b');guard.write(b'0');guard.flush();guard.seek(0)
    try:msvcrt.locking(guard.fileno(),msvcrt.LK_NBLCK,1)
    except OSError:return
    agent=Agent(path)
    while True:
        try:agent.step()
        except Exception as exc:logging.warning('No se completó la comunicación (%s); se reintentará',type(exc).__name__)
        time.sleep(2)


if __name__=='__main__':main()
