import logging
from flask import request
import http_errors
import models
from .helpers import parse_query

def people_post():
    return {"content": models.person.add(request.json)}


def people_get():
    views = ["created"]
    parameters = parse_query(views, request.args)
    return {"content": models.person.query(parameters)}

def person_get(id):
    person = models.person.get(id)
    if person == None:
        raise http_errors.not_found(f"person {id} is not found")
    
    return {"content": person}

def person_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"person {id} does not match resource in body, rejecting"
        )

    person = models.person.update(id, request.json)
    if person == None:
        raise http_errors.unprocessable_content(
            f"person {id} is not found, create using people post"
        )
    else:
        return {"content": person}

def person_delete(id):
    person = models.person.remove(id)
    if person == None:
        raise http_errors.not_found(f"person {id} is not found")

    return {"content": ""}