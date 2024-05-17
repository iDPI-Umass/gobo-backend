import logging
from flask import request
import http_errors
import models


def person_delivery_target_get(person_id, id):
    target = models.delivery_target.find({
        "person_id": person_id,
        "id": id
    })
    
    if target is None:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery target {id}"
        )

    return {
        "content": target
    }