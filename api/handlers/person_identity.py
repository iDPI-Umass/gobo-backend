import logging
from flask import request
import http_errors
import models
from db import tables
from .helpers import get_viewer, parse_page_query

def censor_identity(identity):
    identity.pop("oauth_token", None)
    identity.pop("oauth_token_secret", None)


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
    for identity in identities:
        censor_identity(identity)
    
    return identities

def person_identity_post(person_id, id):
    person = get_viewer(person_id)
    identity = check_claim(person_id, id)

    identity["active"] = request.json["active"]
    identity = models.identity.update(id, identity)
    censor_identity(identity)
    return identity

def person_identity_delete(person_id, id):
    person = get_viewer(person_id)
    check_claim(person_id, id)

    models.link.find_and_remove({
      "origin_type": "person",
      "origin_id": person_id,
      "target_type": "identity",
      "target_id": id,
      "name": "has-identity"
    })
    
    models.identity.remove(id)

    models.task.add({
        "queue": "database",
        "name": "remove identity",
        "details": {
            "identity_id": id
        }
    })

    return ""