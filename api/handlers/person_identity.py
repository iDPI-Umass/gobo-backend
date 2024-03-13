import logging
from flask import request
import http_errors
import models
from db import tables
from .helpers import parse_page_query

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
    query = parse_page_query(request.args)
    query["person_id"] = person_id
    query["resource"] = "identity"

    identities = models.person.get_links(tables.Identity, query)
    for identity in identities:
        censor_identity(identity)
    
    return identities

# NOTE: Conveninece endpoint for now. If there is more stuff associated with the
# identity that we need to model, we should probably establish some subsidary
# resources to model it more independently.
def person_identity_post(person_id, id):
    identity = check_claim(person_id, id)

    identity["active"] = request.json["active"]
    identity = models.identity.update(id, identity)
    censor_identity(identity)
    return identity

def person_identity_delete(person_id, id):
    identity = check_claim(person_id, id)

    models.link.find_and_remove({
      "origin_type": "person",
      "origin_id": person_id,
      "target_type": "identity",
      "target_id": id,
      "name": "has-identity"
    })

    if identity is not None:
        session = models.bluesky_session.find({
            "person_id": identity["person_id"],
            "base_url": identity["base_url"],
            "did": identity["platform_id"]
        })
        if session is not None:
            models.bluesky_session.remove(session["id"])


    models.identity.remove(id)

    models.task.add({
        "queue": "default",
        "name": "remove identity",
        "priority": 10,
        "details": {
            "identity_id": id
        }
    })

    return ""


# I'm running into a weird issue with the naming conventions, so I'm putting
# this resource here to avoid that.
def person_identity_post_get(person_id, identity_id, id):
    identity = models.identity.find({
        "id": identity_id,
        "person_id": person_id
    })
    if identity is None:
        raise http_errors.forbidden(f"cannot view posts for identity {id}")

    result = models.post.view_post_graph({
        "identity_id": identity_id,
        "id": id
    })

    if len(result["feed"]) == 0:
        raise http_errors.not_found(f"post {id} is not found")
    
    return result