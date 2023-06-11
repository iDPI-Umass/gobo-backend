import logging
from flask import request, g
import http_errors
import models
from db import tables
from .helpers import parse_page_query

def get_person(id):
    try:
        person = g.person
    except Exception as e:
        person = models.person.get(id)
    
    return person


def resources_get(id, resource_type):
    query = parse_page_query(request.args)
    query["id"] = id
    person = get_person(id)

    if resource_type == "identities":
        query["resource"] = "identity"
        Table = tables.Identity
    elif resource_type == "filters":
        query["resource"] = "filter"
        Table = tables.Filter
    # elif resource_type == "sources":
    #     query["resource"] = "source"
    #     Table = tables.Source
    else:
        raise http_errors.bad_request(f"{resource_type} is not a recognized person resource")

    resources = models.person.get_links(Table, query)
    return resources

def resource_get(id, resource_type, resource_id):
    person = get_person(id)
    raise http_errors.bad_request("GET on an individual person resource is not currently supported.")

def resource_put(id, resource_type, resource_id):
    person = get_person(id)
    raise http_errors.bad_request("PUT on an individual person resource is not currently supported.")

def resource_delete(id, resource_type, resource_id):
    person = get_person(id)

    if resource_type == "identities":
        target_type = "identity"
        model = models.identity
    elif resource_type == "filters":
        target_type = "filter"
        model = models.filter
    else:
        raise http_errors.bad_request(f"resource {resource_type} does not support individualized DELETE")

    models.link.find_and_remove({
      "origin_type": "person",
      "origin_id": person.id,
      "target_type": target_type,
      "target_id": resource_id,
      "name": f"has-{target_type}"
    })

    def condition (row):
        return row.person_id == id
    
    model.conditional_remove(condition, id)
    return ""