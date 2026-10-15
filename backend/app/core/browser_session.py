"""Cookies persistentes del navegador, limitadas al host que autenticó al usuario."""
from datetime import datetime, timezone
from ipaddress import ip_address


def clear_browser_session(response, request):
    response.delete_cookie('access_token', path='/')
    host=(request.url.hostname or '').lower()
    # Retirar cookies antiguas compartidas, sin intentar escribir en un sufijo público.
    domains=set()
    if host=='hollywoodpacas.ovh' or host.endswith('.hollywoodpacas.ovh'):
        domains.add('.hollywoodpacas.ovh')
    else:
        try:
            ip_address(host)
        except ValueError:
            if len(host.split('.'))>=3:
                domains.add('.'+host.split('.',1)[1])
    for domain in domains:
        response.delete_cookie('access_token',path='/',domain=domain)


def persist_browser_session(response, request, token, duration):
    clear_browser_session(response,request)
    response.set_cookie('access_token',token,httponly=True,samesite='lax',
        secure=request.url.scheme=='https',max_age=int(duration.total_seconds()),
        expires=datetime.now(timezone.utc)+duration,path='/')
