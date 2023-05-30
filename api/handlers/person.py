import logging
from flask import request
import http_errors
import models

def person_get(id):
    person = models.person.get(id)
    if person == None:
        raise http_errors.not_found(f"person {id} is not found")
    
    return person

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
        return person

def person_delete(id):
    person = models.person.remove(id)
    if person == None:
        raise http_errors.not_found(f"person {id} is not found")

    return ""