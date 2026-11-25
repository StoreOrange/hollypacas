"""Import prior successful desktop report opens, without assigning a user identity.
Run with an OS account that can read Nginx logs. Never imports IPs or full user agents.
"""
import argparse,gzip,json,re,os,sys,tempfile
from collections import defaultdict
from datetime import date,datetime
from pathlib import Path
from urllib.parse import urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import app.config  # Load the instance's existing signing key before the audit helper.
from app.core.hot_sales_audit import TZ,TARGET,audit_directory,correlation,mobile_agent,browser_name


def import_history(log_folder,output,start,end):
    grouped=defaultdict(list)
    for path in sorted(Path(log_folder).glob('access.log*')):
        with (gzip.open(path,'rt',errors='replace') if path.suffix=='.gz' else path.open(errors='replace')) as stream:
            for line in stream:
                if TARGET not in line:continue
                match=re.search(r'^(\S+) .*?\[([^]]+)\] "([^"]+)" (\d+) \S+ "([^"]*)" "([^"]*)"',line)
                if not match:continue
                ip,raw_time,request,status,ref,agent=match.groups();parts=request.split()
                try:at=datetime.strptime(raw_time,'%d/%b/%Y:%H:%M:%S %z').astimezone(TZ)
                except ValueError:continue
                if not start<=at.date()<=end or status!='200' or len(parts)<2 or parts[0]!='GET' or urlsplit(parts[1]).path!=TARGET:continue
                if urlsplit(ref).hostname not in {'hollywoodpacas.ovh','www.hollywoodpacas.ovh'} or mobile_agent(agent):continue
                grouped[at.date()].append({'recorded_at':at.isoformat(),'source':'nginx_history','path':TARGET,'status':200,
                                          'user':None,'device':'PC','browser':browser_name(agent),'correlation':correlation(ip,agent)})
    output=Path(output);output.mkdir(mode=0o700,parents=True,exist_ok=True)
    result={}
    for day,rows in sorted(grouped.items()):
        filename=output/('hot-sales-history-'+day.isoformat()+'.jsonl')
        fd,temporary=tempfile.mkstemp(prefix='.history-',dir=output)
        try:
            with os.fdopen(fd,'w') as stream:
                for row in sorted(rows,key=lambda r:r['recorded_at']):stream.write(json.dumps(row,ensure_ascii=False)+'\n')
            os.replace(temporary,filename)
        finally:
            if os.path.exists(temporary):os.unlink(temporary)
        result[day.isoformat()]=len(rows)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--log-directory',default='/var/log/nginx');parser.add_argument('--directory',default=str(audit_directory()))
    parser.add_argument('--start',required=True,type=date.fromisoformat);parser.add_argument('--end',required=True,type=date.fromisoformat)
    args=parser.parse_args()
    if args.end<args.start:parser.error('Invalid range')
    print(json.dumps(import_history(args.log_directory,args.directory,args.start,args.end),ensure_ascii=False))
