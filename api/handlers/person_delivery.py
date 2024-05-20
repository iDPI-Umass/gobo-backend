import logging
from flask import request
import http_errors
import models


def parse_feed_query():
    data = request.args

    try:
        per_page = int(data.get("per_page") or 25)
    except Exception as e:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")

    if per_page < 1:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")
    if per_page > 100:
        per_page = 100

    
    start = data.get("start")

    return {
      "per_page": per_page,
      "start": start,
    }


def person_deliveries_post(person_id):
    draft_id = request.json["draft_id"]
    draft = models.draft.find({
        "person_id": person_id,
        "id": draft_id
    })
    if draft is None:
        raise http_errors.not_found(
            f"person {person_id} does not have draft {draft_id}"
        ) 
    
    proof_id = request.json["proof_id"]
    proof = models.proof.find({
        "person_id": person_id,
        "id": proof_id
    })
    if proof is None:
        raise http_errors.not_found(
            f"person {person_id} does not have proof {proof_id}"
        ) 

    delivery = models.delivery.add({
        "person_id": person_id,
        "draft_id": draft_id,
        "proof_id": proof_id
    })

    return {
        "content": delivery
    }

def person_deliveries_get(person_id):
    query = parse_feed_query()
    query["person_id"] = person_id
    query["identity_id"] = id

    list = models.delivery.view_person(query)

    return {"content": list}


def person_delivery_get(person_id, id):
    graph = models.delivery.fetch(id)
    
    if len(graph["deliveries"]) == 0:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )
    if graph["deliveries"][0]["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )

    return {
        "content": graph
    }



def unpublish_action(person_id, id):
    graph = models.delivery.fetch(id)
    
    if len(graph["deliveries"]) == 0:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )
    if graph["deliveries"][0]["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )
    

    for target in graph["targets"]:
        identity = models.identity.find({
            "person_id": person_id,
            "id": target["identity_id"]
        })
        if identity is None:
            continue
        if identity["stale"] == True:
            continue
        if target["state"] != "delivered":
            continue
        

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


def person_delivery_post(person_id, id):
    action = request.json["action"]
    
    if action == "unpublish":
        unpublish_action(person_id, id)
    else:
        raise http_errors.bad_request(
            f"action {action} is not recognized"
        )   

    # (for now) no content response
    return {"content": ""}