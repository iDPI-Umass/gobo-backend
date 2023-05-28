import logging
from flask import request
import models

def people_post():
    json = request.json
    return models.person.add(json)

def people_get():
    list = models.person.list()

    return {
      "list": list
    }