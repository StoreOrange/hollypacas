from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    LOCAL_TZ = ZoneInfo("America/Managua")
except ZoneInfoNotFoundError:
    # Fallback for environments without tzdata (e.g., some Windows installs)
    LOCAL_TZ = timezone(timedelta(hours=-6))


def local_now() -> datetime:
    return datetime.now(tz=LOCAL_TZ)


def local_now_naive() -> datetime:
    return local_now().replace(tzinfo=None)


def local_today():
    return local_now().date()
