import logging
from flask import request
import http_errors
import models
from .helpers import parse_query


def filters_post():
    return models.filter.add(request.json)

def filters_get():
    views = ["created", "base_url"]
    parameters = parse_query(views, request.args)
    return models.filter.query(parameters)

def filter_get(id):
    filter = models.filter.get(id)
    if filter == None:
        raise http_errors.not_found(f"filter {id} is not found")
    
    return filter

def filter_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"filter {id} does not match resource in body, rejecting"
        )

    filter = models.filter.update(id, request.json)
    if filter == None:
        raise http_errors.unprocessable_content(
            f"filter {id} is not found, create using people post"
        )
    else:
        return filter

def filter_delete(id):
    filter = models.filter.remove(id)
    if filter == None:
        raise http_errors.not_found(f"filter {id} is not found")

    return ""