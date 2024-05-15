import logging
from flask import request
import http_errors
import models



def person_delivery_post_delete(person_id, delivery_id, identity_id):
    delivery = models.delivery.fetch(delivery_id)
    
    if delivery is None:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )
    if delivery["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )

    match = None
    for target in delivery["targets"]:
        if target["identity"] == identity_id:
            match = target
            break
    
    if match is None:
        raise http_errors.not_found(
            f"delivery {delivery_id} does not have target identity {identity_id}"
        )

    return {"content": ""}