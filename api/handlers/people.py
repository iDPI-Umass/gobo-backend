import logging
from flask import request
import models

def people_post():
    models.person.add(request.json)
    return { "foo": "foo" }

def people_get():
    return { "foo": "foo" }