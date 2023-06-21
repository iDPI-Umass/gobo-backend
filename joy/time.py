from datetime import datetime

def now():
    timestamp = datetime.utcnow().isoformat(timespec="milliseconds")
    return f"{timestamp}Z"

def to_iso_string(d):
    timestamp = d.isoformat(timespec="milliseconds")
    if timestamp.endswith("+00:00"):
        return timestamp.replace("+00:00", "Z")
    elif timestamp.endswith("Z"):
        return timestamp
    else:
        return f"{timestamp}Z"

def unix_to_iso(unix):
    _date = datetime.fromtimestamp(unix)
    return to_iso_string(_date)