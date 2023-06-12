import logging
from flask import request
import http_errors
import models
from .helpers import parse_query


def lenses_post():
    return models.lens.add(request.json)

def lenses_get():
    views = ["created", "base_url"]
    parameters = parse_query(views, request.args)
    return models.lens.query(parameters)

def lens_get(id):
    lens = models.lens.get(id)
    if lens == None:
        raise http_errors.not_found(f"lens {id} is not found")
    
    return lens

def lens_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"lens {id} does not match resource in body, rejecting"
        )

    lens = models.lens.update(id, request.json)
    if lens == None:
        raise http_errors.unprocessable_content(
            f"lens {id} is not found, create using people post"
        )
    else:
        return lens

def lens_delete(id):
    lens = models.lens.remove(id)
    if lens == None:
        raise http_errors.not_found(f"lens {id} is not found")

    return ""