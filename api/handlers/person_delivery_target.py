import logging
from flask import request
import http_errors
import models


def person_delivery_target_get(person_id, id):
    target = models.delivery_target.find({
        "person_id": person_id,
        "id": id
    })
    
    if target is None:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery target {id}"
        )

    return {
        "content": target
    }


def unpublish_action(person_id, id):
    target = models.delivery_target.find({
        "person_id": person_id,
        "id": id
    })
    
    if target is None:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery target {id}"
        )
    
    # TODO: Make this more sophisticated.
    if target["state"] != "delivered":
        return

    identity = models.identity.find({
        "person_id": person_id,
        "id": target["identity_id"]
    })
    if identity is None:
        raise http_errors.not_found(
            f"person {person_id} does not have identity {target["identity_id"]}"
        )
    if identity["stale"] == True:
        return

  
    target["state"] = "pending"
    target = models.delivery_target.update(target["id"], target)
    models.task.add({
        "queue": identity["platform"],
        "name": "unpublish post",
        "priority": 1,
        "details": {
          "target": target,
          "identity": identity,
        }
    })


def person_delivery_target_post(person_id, id):
    action = request.json["action"]
    
    if action == "unpublish":
        unpublish_action(person_id, id)
    else:
        raise http_errors.bad_request(
            f"action {action} is not recognized"
        )   

    # (for now) no content response
    return {"content": ""}