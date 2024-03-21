import logging
from flask import request
from api_specification import spec

def discovery_get():
    return {
        "content": {
            "resources": spec["resources"]
        },
        "headers": {
            "cache-control": "max-age=60, s-maxage=31536000"
        }
    }
