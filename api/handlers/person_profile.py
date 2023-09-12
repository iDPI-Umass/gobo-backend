import logging
from flask import request
import http_errors
import models


def person_profile_get(person_id):
    person = models.person.get(person_id)
    if person == None:
        raise http_errors.not_found(f"person {person_id} is not found")
    
    return person

def person_profile_put(person_id):
    if request.json["id"] is not None and person_id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"person {person_id} does not match resource in body, rejecting"
        )

    person = models.person.update(person_id, request.json)
    if person == None:
        raise http_errors.unprocessable_content(
            f"person {person_id} is not found, create using people post"
        )
    else:
        return person