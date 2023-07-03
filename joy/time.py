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

def to_unix(d):
    return int(d.timestamp())


def convert(start, end, value):
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