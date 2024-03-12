import logging
import secrets
import base64
import numpy

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


# Base 36 addresses are guaranteed to be URL safe while maintaining relatively
# short expression for their byte length.
def address ( length = 16 ):
    power = length - 1
    result = 0
    for byte in secrets.token_bytes(length):
        result += byte * (256 ** power)
        power -= 1

    return numpy.base_repr(result, base=36).lower()