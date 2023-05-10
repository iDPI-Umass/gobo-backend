from datetime import datetime

def now():
    return datetime.utcnow().isoformat(timespec="milliseconds")