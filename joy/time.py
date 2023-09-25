import logging
from datetime import datetime, timezone, timedelta

def nowdate():
    return datetime.now(timezone.utc)

def now():
    return nowdate().isoformat(timespec="milliseconds")

def to_iso_string(d):
    timestamp = d.isoformat(timespec="milliseconds")
    if timestamp.endswith("+00:00"):
        return timestamp.replace("+00:00", "Z")
    elif timestamp.endswith("Z"):
        return timestamp
    else:
        return f"{timestamp}Z"

def to_unix(d):
    return int(d.timestamp())


def convert(start, end, value, optional = False):
    if optional == True and value is None:
        return None
    if value is None:
        raise Exception("joy.time.convert: time value was not provided")

    if start == "date":
        d = value
    elif start == "iso":
        d = datetime.fromisoformat(value)
    elif start == "unix":
        d = datetime.fromtimestamp(value)
    else:
        raise Exception(f"unsupported start encoding {start}")

    if end == "date":
        return d
    elif end == "iso":
        return to_iso_string(d)
    elif end == "unix":
        return to_unix(d)
    else:
        raise Exception(f"unsupported end encoding {end}")
    
def hours_ago(hours):
    return nowdate() - timedelta(hours = hours)

def hours_from_now(hours):
    return nowdate() + timedelta(hours = hours)

def remaining(d):
    return d - nowdate()

def latency(d):
    return nowdate() - d

def timer():
    start = nowdate()
    def _timer():
        return nowdate() - start
    return _timer