from datetime import datetime
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/Managua")


def local_now() -> datetime:
    return datetime.now(tz=LOCAL_TZ)


def local_now_naive() -> datetime:
    return local_now().replace(tzinfo=None)


def local_today():
    return local_now().date()
