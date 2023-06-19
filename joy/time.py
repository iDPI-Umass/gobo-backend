from datetime import datetime

def now():
    timestamp = datetime.utcnow().isoformat(timespec="milliseconds")
    return f"{timestamp}Z"