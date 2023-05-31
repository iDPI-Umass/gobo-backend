import logging
from flask import request
from api_specification import spec

def discovery_get():
    return {"resources": spec["resources"]}
