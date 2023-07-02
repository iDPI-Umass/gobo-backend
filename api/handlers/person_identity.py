import logging
from flask import request
import http_errors
import models
from db import tables
from .helpers import get_viewer, parse_page_query


def check_claim(person_id, id):
    identity = models.identity.get(id)
    
    if identity == None or identity["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have an identity with ID {id}"
        )
    
    return identity


def person_identities_get(person_id):
    person = get_viewer(person_id)
    query = parse_page_query(request.args)
    query["person_id"] = person_id
    query["resource"] = "identity"

    identities = models.person.get_links(tables.Identity, query)
    return identities

def person_identity_post(person_id, id):
    person = get_viewer(person_id)
    identity = check_claim(person_id, id)

    identity["active"] = request.json["active"]
    identity = models.identity.update(id, identity)
    return identity

def person_identity_delete(person_id, id):
    person = get_viewer(person_id)
    check_claim(person_id, id)

    identity = models.identity.get(id)

    models.link.find_and_remove({
      "origin_type": "person",
      "origin_id": person_id,
      "target_type": "identity",
      "target_id": id,
      "name": "has-identity"
    })
    
    models.identity.remove(id)

    if identity is not None:
        models.task.add({
            "queue": "database",
            "name": "remove identity",
            "details": {
                "identity": identity
            }
        })

    return ""