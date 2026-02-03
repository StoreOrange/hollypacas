from datetime import datetime, date


def local_now() -> datetime:
    # Use server-local time (server already configured to America/Managua)
    return datetime.now()


def local_now_naive() -> datetime:
    return local_now()


def local_today():
    return date.today()
