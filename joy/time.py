from datetime import datetime

def now():
    timestamp = datetime.utcnow().isoformat(timespec="milliseconds")
    return f"{timestamp}Z"

def to_iso_string(d):
    timestamp = d.isoformat(timespec="milliseconds")
    return timestamp.replace("+00:00", "Z")