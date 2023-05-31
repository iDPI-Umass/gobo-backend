import logging
from flask import request
import http_errors
import models
from .helpers import parse_query


def registrations_post():
    return models.registration.add(request.json)

def registrations_get():
    views = ["created", "base_url"]
    parameters = parse_query(views, request.args)
    return models.registration.query(parameters)

def registration_get(id):
    registration = models.registration.get(id)
    if registration == None:
        raise http_errors.not_found(f"registration {id} is not found")
    
    return registration

def registration_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"registration {id} does not match resource in body, rejecting"
        )

    registration = models.registration.update(id, request.json)
    if registration == None:
        raise http_errors.unprocessable_content(
            f"registration {id} is not found, create using people post"
        )
    else:
        return registration

def registration_delete(id):
    registration = models.registration.remove(id)
    if registration == None:
        raise http_errors.not_found(f"registration {id} is not found")

    return ""