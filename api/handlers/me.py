import logging
from flask import request, g
import http_errors
import models
from .helpers import parse_query

def me_get():
    person = getattr(g, "person", None)
    if person is None:
        authority_id = g.claims["sub"]
        person = models.person.lookup(authority_id)
    return {
        "content": person
    }

def me_put():
    authority_id = g.claims["sub"]
    person = models.person.find({"authority_id": authority_id})
    if person == None:
        person = models.person.add({"authority_id": authority_id})

    person["name"] = request.json["name"]
    person = models.person.update(person["id"], person)
    return {
        "content": person
    }