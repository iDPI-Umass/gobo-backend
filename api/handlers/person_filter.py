import logging
from flask import request
import http_errors
import models
from db import tables
from .helpers import parse_page_query


def check_claim(person_id, id):
    filter = models.filter.get(id)

    if filter == None or filter["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have a filter with ID {id}"
        )

    return filter

def person_filters_get(person_id):
    query = parse_page_query(request.args)
    query["person_id"] = person_id
    query["resource"] = "filter"

    filters = models.person.get_links(tables.Filter, query)
    return filters

def person_filters_post(person_id):
    data = dict(request.json)
    data["person_id"] = person_id

    filter = models.filter.add(data)
    models.link.add({
        "origin_type": "person",
        "origin_id": person_id,
        "target_type": "filter",
        "target_id": filter["id"],
        "name": "has-filter"
    })

    return filter


def person_filter_put(person_id, id):
    data = dict(request.json)
    data["person_id"] = person_id
    check_claim(person_id, id)
    filter = models.filter.update(id, data)
    return filter

def person_filter_delete(person_id, id):
    check_claim(person_id, id)

    models.link.find_and_remove({
      "origin_type": "person",
      "origin_id": person_id,
      "target_type": "filter",
      "target_id": id,
      "name": "has-filter"
    })
    
    models.filter.remove(id)
    return ""