import logging
from flask import request
import http_errors
import models
from .helpers import get_viewer

valid_views = ["full"]

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

def person_identity_feed_get(person_id, id):
    person = get_viewer(person_id)
    query = parse_feed_query()
    query["person_id"] = person_id
    query["identity_id"] = id

    identity = models.identity.get(id)
    if identity == None or identity["person_id"] != person_id:
        raise http_errors.not_found(
            f"identity feed /people/{person_id}/identities/{id} is not found"
        )


    return models.post.view_identity_feed(query)