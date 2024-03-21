import logging
from flask import request
import http_errors
import models


def person_profile_get(person_id):
    person = models.person.get(person_id)
    if person == None:
        raise http_errors.not_found(f"person {person_id} is not found")
    
    return {"content": person}

def person_profile_put(person_id):
    id = request.json.get("id", None)
    if id is None or id != person_id:
        raise http_errors.unprocessable_content(
            f"person {person_id} does not match resource in body, rejecting"
        )
    
    data = {
        "id": person_id,
        "name": request.json.get("name", None),
        "authority_id": request.json.get("authority_id", None)
    }
   
    person = models.person.update(person_id, data)
    if person == None:
        raise http_errors.unprocessable_content(
            f"person {person_id} is not found, create using people post"
        )
    else:
        return {"content": person}