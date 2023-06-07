import secrets
import base64

def id (length):
    return base64.b64encode(secrets.token_bytes(16))
