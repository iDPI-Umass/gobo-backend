import logging
from flask import request
import http_errors
import models
from db import tables
from .helpers import get_viewer, parse_page_query

allowed_stores = [
    "feed",
    "welcome"
]


def person_store_get(person_id, name):
    if name not in allowed_stores:
        raise http_errors.not_found(f"store {name} is not found")

    store = models.store.find({
        "person_id": person_id,
        "name": name
    })

    if store is None:
        raise http_errors.not_found(f"store {name} is not found")
    
    return store


def person_store_put(person_id, name):
    if name not in allowed_stores:
        raise http_errors.forbidden(f"not allowed to edit {name}")

    data = dict(request.json)
    data["person_id"] = person_id
    data["name"] = name
    store = models.store.upsert(data)
    return store

def person_store_delete(person_id, name):
    store = models.store.find({
        "person_id": person_id,
        "name": name
    })

    if store is not None:
        models.store.remove(store["id"])

    return ""