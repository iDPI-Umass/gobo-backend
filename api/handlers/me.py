import logging
from flask import request, g
import http_errors
import models
from .helpers import parse_query

def me_get():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    return person

def me_put():
    authority_id = g.claims["sub"]
    person = models.person.find({"authority_id": authority_id})
    if person == None:
        person = models.person.add({"authority_id": authority_id})

    person["name"] = request.json["name"]
    person = models.person.update(person["id"], person)
    return person