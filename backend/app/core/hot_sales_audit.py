"""Read-only access reporting, separate from invoices, inventory and financial reports."""
from __future__ import annotations
import hashlib,hmac,json,logging,os,re,secrets
from bisect import bisect_left
from collections import defaultdict
from datetime import date,datetime,timedelta
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo
from .security import SECRET_KEY

TZ=ZoneInfo('America/Managua')
TARGET='/sales/ventas-caliente'
LOGGER=logging.getLogger(__name__)


def audit_directory():
    return Path(os.getenv('HOT_SALES_AUDIT_DIR') or os.getenv('MOBILE_AUDIT_DIR') or Path(__file__).resolve().parents[2]/'private_audit')


def correlation(ip,agent):
    return hmac.new(SECRET_KEY.encode(),(str(ip or '')+'\n'+str(agent or '')[:400]).encode(),hashlib.sha256).hexdigest()[:32]


def mobile_agent(agent):
    return bool(re.search(r'Android|iPhone|iPad|iPod|Mobile|IEMobile',agent,re.I))


def browser_name(agent):
    return ('Edge' if re.search(r'Edg|EdgiOS|EdgA',agent) else 'Firefox' if re.search(r'Firefox|FxiOS',agent)
            else 'Chrome' if re.search(r'Chrome|CriOS',agent) else 'Safari' if 'Safari' in agent else 'Otro')


def record_access(request,user):
    """Call only after a successful, authenticated report render. Failure never blocks sales."""
    try:
        now=datetime.now(TZ);agent=request.headers.get('user-agent','')[:400]
        row={'recorded_at':now.isoformat(timespec='milliseconds'),'source':'hot_sales_server','status':200,
             'path':TARGET,'access_id':secrets.token_hex(12),'user':user.email,'name':user.full_name or user.email,
             'device':'Móvil' if mobile_agent(agent) or request.headers.get('sec-ch-ua-mobile')=='?1' else 'PC',
             'browser':browser_name(agent),'correlation':correlation(request.client.host if request.client else '',agent)}
        folder=audit_directory();folder.mkdir(mode=0o700,parents=True,exist_ok=True)
        fd=os.open(folder/('hot-sales-'+now.date().isoformat()+'.jsonl'),os.O_APPEND|os.O_CREAT|os.O_WRONLY,0o600)
        try:os.write(fd,(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n').encode())
        finally:os.close(fd)
    except (OSError,TypeError,ValueError):
        LOGGER.exception('No se pudo registrar el acceso al informe de ventas en caliente')


def parse_time(value):
    try:
        stamp=datetime.fromisoformat(str(value).replace('Z','+00:00'))
        return stamp.astimezone(TZ) if stamp.tzinfo else stamp.replace(tzinfo=TZ)
    except (ValueError,TypeError):return None


@lru_cache(maxsize=192)
def _read_file(filename,modified,size):
    result=[]
    with Path(filename).open(encoding='utf-8') as stream:
        for line in stream:
            try:
                raw=json.loads(line)
                if not isinstance(raw,dict):continue
                stamp=parse_time(raw.get('recorded_at'))
                if not stamp:continue
                source=raw.get('source')
                if source in {'hot_sales_server','nginx_history'}:
                    if raw.get('status')!=200 or raw.get('path')!=TARGET:continue
                    kind='apertura';device=raw.get('device','PC');key=raw.get('correlation','')
                else:
                    if raw.get('url',{}).get('path')!=TARGET:continue
                    if source=='server' and raw.get('event')=='http_request' and raw.get('status')==200:kind='apertura'
                    elif raw.get('event') in {'page_view','pageshow'} and raw.get('navigation') in {'back_forward','bfcache'}:kind='regreso'
                    else:continue
                    device='Móvil';key=correlation(raw.get('ip'),raw.get('device_declared',''))
                result.append({'at':stamp,'user':str(raw.get('user') or '').strip().lower(),
                    'name':str(raw.get('name') or ''),'device':device,'browser':raw.get('browser') or raw.get('browser_declared') or '',
                    'kind':kind,'source':source,'key':key,'id':raw.get('access_id') or raw.get('request_id') or str(len(result))})
            except (ValueError,TypeError,AttributeError):continue
    return tuple(result)


def load_day(folder,day):
    events=[];warnings=[];files=0
    for prefix in ['hot-sales-','mobile-','hot-sales-history-']:
        path=Path(folder)/(prefix+day.isoformat()+'.jsonl')
        try:
            stat=path.stat();events.extend(dict(row) for row in _read_file(str(path),stat.st_mtime_ns,stat.st_size) if row['at'].date()==day);files+=1
        except FileNotFoundError:continue
        except OSError:warnings.append('No se pudo leer uno de los registros del '+day.strftime('%d/%m/%Y'))
    # A new authenticated entry may also occur in the pre-existing mobile middleware.
    # Pair entries one-to-one; never collapse two legitimate reloads of the same report.
    canonical=defaultdict(list)
    for event in events:
        if event['source']=='hot_sales_server' and event['key']:canonical[event['key']].append(event)
    for group in canonical.values():group.sort(key=lambda row:row['at'])
    used=defaultdict(set);kept=[]
    for event in sorted(events,key=lambda row:row['at']):
        if event['source'] in {'server','nginx_history'} and event['key']:
            group=canonical[event['key']];stamps=[row['at'] for row in group];index=bisect_left(stamps,event['at'])
            candidates=[(abs((group[i]['at']-event['at']).total_seconds()),i) for i in range(max(0,index-2),min(len(group),index+3))
                        if i not in used[event['source'],event['key']] and (not event['user'] or event['user']==group[i]['user'])]
            if candidates:
                distance,i=min(candidates)
                if distance<=3:used[event['source'],event['key']].add(i);continue
        kept.append(event)
    return kept,warnings,files


def duration(seconds):
    if seconds is None:return '—'
    seconds=max(0,round(seconds));hours,seconds=divmod(seconds,3600);minutes,seconds=divmod(seconds,60)
    return (f'{hours} h ' if hours else '')+(f'{minutes} min ' if minutes or hours else '')+f'{seconds} s'


def build_report(start,end,*,account='',device='',kind='apertura',folder=None,names=None):
    folder=folder or audit_directory();names=names or {};events=[];warnings=[];days_with_logs=[]
    cursor=start
    while cursor<=end:
        day_events,day_warnings,count=load_day(folder,cursor);events.extend(day_events);warnings.extend(day_warnings)
        if count:days_with_logs.append(cursor)
        if cursor==end:break
        cursor+=timedelta(days=1)
    all_users=sorted({r['user'] for r in events if r['user']})
    filtered=[r for r in events if (not account or (not r['user'] if account=='__unknown__' else r['user']==account.lower()))
              and (not device or r['device']==device) and (kind=='todas' or r['kind']==kind)]
    filtered.sort(key=lambda r:(r['at'],r['id']))
    previous={};counts=defaultdict(int);summaries={}
    for row in filtered:
        user=row['user'];key=(user,row['at'].date());row['display_name']=names.get(user) or row['name'] or user or 'Sin usuario identificado'
        row['interval']=None;row['ordinal']=None
        if user:
            row['interval']=(row['at']-previous[key]).total_seconds() if key in previous else None
            previous[key]=row['at'];counts[key]+=1;row['ordinal']=counts[key]
        label=user or '__unknown__'
        summary=summaries.setdefault(label,{'name':row['display_name'],'user':user,'opens':0,'returns':0,'first':row['at'],'last':row['at'],'gaps':[]})
        summary['opens']+=row['kind']=='apertura';summary['returns']+=row['kind']=='regreso';summary['last']=row['at']
        if row['interval'] is not None:summary['gaps'].append(row['interval'])
    for summary in summaries.values():summary['average']=sum(summary['gaps'])/len(summary['gaps']) if summary['gaps'] else None
    missing=[];cursor=start
    while cursor<=end:
        if cursor not in days_with_logs:missing.append(cursor)
        if cursor==end:break
        cursor+=timedelta(days=1)
    return {'missing_days':missing,'rows':filtered,'summaries':list(summaries.values()),'users':[(u,names.get(u) or u) for u in all_users],
            'opens':sum(r['kind']=='apertura' for r in filtered),'returns':sum(r['kind']=='regreso' for r in filtered),
            'unknown':sum(not r['user'] for r in filtered),'mobile':sum(r['device']=='Móvil' for r in filtered),'pc':sum(r['device']=='PC' for r in filtered),
            'warnings':warnings,'days_with_logs':days_with_logs,'first':filtered[0]['at'] if filtered else None,'last':filtered[-1]['at'] if filtered else None}
