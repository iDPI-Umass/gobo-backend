import logging
from flask import request
import http_errors
import models


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