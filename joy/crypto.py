import secrets
import base64

def random (configuration = None):
    if configuration == None:
        configuration = {}

    length = configuration.get("length", 16)
    encoding = configuration.get("encoding", "base64")
    value = secrets.token_bytes(length)

    if encoding == "base64":
        value = base64.b64encode(value)
    elif encoding == "safe-base64":
        value = base64.urlsafe_b64encode(value)
    elif encoding == "base32":
        value = base64.b32encode(value)
    elif encoding in ["base16", "hex"]:
        value = base64.b16encode(value)
    else:
        raise Exception(f"{encoding} is not a recognized encoding")

    return value.decode("utf-8")
