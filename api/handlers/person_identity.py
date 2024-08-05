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
    
    return {"content": identities}

# NOTE: Conveninece endpoint for now. If there is more stuff associated with the
# identity that we need to model, we should probably establish some subsidary
# resources to model it more independently.
def person_identity_post(person_id, id):
    identity = check_claim(person_id, id)

    identity["active"] = request.json["active"]
    identity = models.identity.update(id, identity)
    censor_identity(identity)
    return {"content": identity}

# While we censor identities as a list, part of the overall state fetching, if
# a client asks for the individual identity, we don't censor sensitive values
# because they're needed for some reason.
# TODO: consider if we want to spin out this full representation into some
# other resource space apart from this one.
def person_identity_get(person_id, id):
    identity = models.identity.find({
        "person_id": person_id,
        "id": id,
    })
    if identity is None:
        raise http_errors.forbidden(f"person_identity {person_id}/{id} is not found")
    
    return {"content": identity}

def person_identity_delete(person_id, id):
    identity = models.identity.find({
        "person_id": person_id,
        "id": id,
    })
    if identity is None:
        raise http_errors.forbidden(f"person_identity {person_id}/{id} is not found")

    # Sever the link so we affect the HX right away...
    models.link.find_and_remove({
      "origin_type": "person",
      "origin_id": person_id,
      "target_type": "identity",
      "target_id": id,
      "name": "has-identity"
    })

    # ...but remove the associated resources with the worker.
    models.task.add({
        "queue": "default",
        "name": "remove identity",
        "priority": 1,
        "details": {
            "identity": identity
        }
    })

    return {"content": ""}


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
    
    return {"content": result}