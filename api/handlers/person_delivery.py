import logging
from flask import request
import http_errors
import models


def person_delivery_get(person_id, id):
    delivery = models.delivery.fetch(id)
    
    if delivery is None:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )
    if delivery["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )

    return {
        "content": delivery
    }