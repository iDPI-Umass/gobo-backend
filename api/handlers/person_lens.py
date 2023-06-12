import logging
from flask import request
import http_errors
import models
from db import tables
from .helpers import get_viewer, parse_page_query


def check_claim(person_id, id):
    lens = models.lens.get(id)

    if lens == None or lens["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have a lens with ID {id}"
        )

    return lens

def person_lenses_get(person_id):
    person = get_viewer(person_id)
    query = parse_page_query(request.args)
    query["person_id"] = person_id
    query["resource"] = "lens"

    lenses = models.person.get_links(tables.Lens, query)
    return lenses

def person_lenses_post(person_id):
    person = get_viewer(person_id)
    data = dict(request.json)
    data["person_id"] = person_id

    lens = models.lens.add(data)
    models.link.add({
        "origin_type": "person",
        "origin_id": person["id"],
        "target_type": "lens",
        "target_id": lens["id"],
        "name": "has-lens"
    })

    return lens


def person_lens_put(person_id, id):
    person = get_viewer(person_id)
    data = dict(request.json)
    data["person_id"] = person_id
    check_claim(person_id, id)
    lens = models.lens.update(id, data)
    return lens

def person_lens_delete(person_id, id):
    person = get_viewer(person_id)
    check_claim(person_id, id)

    models.link.find_and_remove({
      "origin_type": "person",
      "origin_id": person_id,
      "target_type": "lens",
      "target_id": id,
      "name": "has-lens"
    })
    
    models.lens.remove(id)
    return ""