import logging
from flask import request
import http_errors
import models
from .helpers import parse_query


def sources_post():
    url = request.json["url"]
    source = models.source.find({"url": url})
    if source != None:
        raise http_errors.conflict(f"source {url} is already registered")

    return models.source.add(request.json)

def sources_get():
    views = ["created"]
    parameters = parse_query(views, request.args)
    return models.source.query(parameters)

def source_get(id):
    source = models.source.get(id)
    if source == None:
        raise http_errors.not_found(f"source {id} is not found")
    
    return source

def source_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"source {id} does not match resource in body, rejecting"
        )

    source = models.source.update(id, request.json)
    if source == None:
        raise http_errors.unprocessable_content(
            f"source {id} is not found, create using people post"
        )
    else:
        return source

def source_delete(id):
    source = models.source.remove(id)
    if source == None:
        raise http_errors.not_found(f"source {id} is not found")

    return ""