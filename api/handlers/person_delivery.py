import logging
from flask import request
import http_errors
import models

def parse_feed_query():
    data = request.args

    try:
        per_page = int(data.get("per_page") or 25)
    except Exception as e:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")

    if per_page < 1:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")
    if per_page > 100:
        per_page = 100

    
    start = data.get("start")

    return {
      "per_page": per_page,
      "start": start,
    }


def person_deliveries_post(person_id):
    delivery = models.delivery.add({
        "person_id": person_id
    })

    return {
        "content": delivery
    }

def person_deliveries_get(person_id):
    query = parse_feed_query()
    query["person_id"] = person_id
    query["identity_id"] = id

    list = models.delivery.view_person(query)

    return {"content": list}


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