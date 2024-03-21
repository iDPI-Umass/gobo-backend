import logging
from flask import request
import http_errors
import models
from .helpers import parse_query

def gobo_keys_post():
    return {"content": models.gobo_key.add(request.json)}


def gobo_keys_get():
    views = ["created"]
    parameters = parse_query(views, request.args)
    return {"content": models.gobo_key.query(parameters)}

def gobo_key_get(id):
    key = models.gobo_key.get(id)
    if key == None:
        raise http_errors.not_found(f"gobo_key {id} is not found")
    
    return {"content": key}

def gobo_key_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"gobo_key {id} does not match resource in body, rejecting"
        )

    key = models.gobo_key.update(id, request.json)
    if key == None:
        raise http_errors.unprocessable_content(
            f"gobo_key {id} is not found, create using gobo_keys post"
        )
    else:
        return {"content": key}

def gobo_key_delete(id):
    key = models.gobo_key.remove(id)
    if key == None:
        raise http_errors.not_found(f"gobo_key {id} is not found")

    return {"content": ""}