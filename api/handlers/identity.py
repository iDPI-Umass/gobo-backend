import logging
from flask import request
import http_errors
import models
from .helpers import parse_query, parse_base_url


def identities_post():
    parse_base_url(request.json)
    person_id = request.json["person_id"]
    profile_url = request.json["profile_url"]
    identity = models.identity.find({
        "person_id": person_id,
        "profile_url": profile_url
    })
    if identity != None:
        raise http_errors.conflict(f"profile {profile_url} is already registered")

    return models.identity.add(request.json)

def identities_get():
    views = ["created", "base_url"]
    parameters = parse_query(views, request.args)
    return models.identity.query(parameters)

def identity_get(id):
    identity = models.identity.get(id)
    if identity == None:
        raise http_errors.not_found(f"identity {id} is not found")
    
    return identity

def identity_put(id):
    parse_base_url(request.json)
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"identity {id} does not match resource in body, rejecting"
        )

    identity = models.identity.update(id, request.json)
    if identity == None:
        raise http_errors.unprocessable_content(
            f"identity {id} is not found, create using people post"
        )
    else:
        return identity

def identity_delete(id):
    identity = models.identity.remove(id)
    if identity == None:
        raise http_errors.not_found(f"identity {id} is not found")

    return ""