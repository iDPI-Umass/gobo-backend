import logging
from flask import request
import models
from .helpers import parse_query

def people_post():
    return models.person.add(request.json)


def people_get():
    views = ["created"]
    parameters = parse_query(views, request.args)
    return models.person.list(parameters)