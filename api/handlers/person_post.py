import logging
from flask import request
import http_errors
import models
from platform_models import bluesky, reddit

def get_unfurl_image(person_id, image):
    id = image.get("id")
    if id is None:
        raise http_errors.bad_request(
            f"bluesky link unfurl image was not associated with uploaded image"
        )
    else:
        file = models.draft_file.find({
            "id": id,
            "person_id": person_id
        })
        if file is None:
            raise http_errors.bad_request(
                f"bluesky link unfurl image upload {person_id}/{id} is not found"
            )
        else:
            return file


def person_posts_post(person_id):
    delivery_id = request.json["delivery_id"]
    delivery = models.delivery.find({
        "person_id": person_id,
        "id": delivery_id
    })
    if delivery is None:
        raise http_errors.bad_request(
            f"person {person_id} does not have delivery {id}"
        )
    draft_id = delivery.get("draft_id")
    if draft_id is None:
        raise http_errors.bad_request(
            f"delivery is incomplete and lacks a draft id"
        )
    proof_id = delivery.get("proof_id")
    if proof_id is None:
        raise http_errors.bad_request(
            f"delivery is incomplete and lacks a proof id"
        )
    

    draft = models.draft.find({
        "person_id": person_id,
        "id": draft_id
    })
    if draft is None:
        raise http_errors.bad_request(
            f"person {person_id} does not have draft {id}"
        )
    

    proof = models.proof.find({
        "person_id": person_id,
        "id": proof_id
    })
    if proof is None:
        raise http_errors.bad_request(
            f"person {person_id} does not have proof {id}"
        )
    

    metadata = {}
    identity_ids = []
    for target in request.json["targets"]:
        metadata[target["identity"]] = target.get("stash", {})
        identity_ids.append(target["identity"])

    identities = {}
    results = models.identity.pull([ 
        models.helpers.where("id", identity_ids, "in")
    ])
    for result in results:
        identities[result["id"]] = result

    # Confirm requestor has access to these identities.
    for id in identity_ids:
        identity = identities.get(id, None)
        if identity is None:
            raise http_errors.forbidden(
                f"cannot publish to identity {id}"
            )
        if identity["stale"] == True:
            raise http_errors.bad_request(
                f"identity {id} is stale"
            )
        if identity["person_id"] != person_id:
            raise http_errors.bad_request(
                f"cannot publish to identity {id}"
            )
    

    # Establish post data core.
    post = {
        "title": proof.get("title"),
        "content": proof.get("content"),
        "poll": proof.get("poll")
    }


    # Confirm attachments have been uploaded already.
    file_ids = proof.get("files", [])
    attachments = []
    for id in file_ids:
        file = models.draft_file.find({
            "id": id,
            "person_id": person_id
        })

        if file is None:
            raise http_errors.bad_request(
                f"draft image {person_id}/{id} is not found"
            )
       
        attachments.append(file)

    post["attachments"] = attachments


    # Bluesky and Linkedin need the link card image uplaoded independently.
    for id in identity_ids:
        link_card = metadata[id].get("linkCard")
        if link_card is not None:
            image = link_card.get("image")
            if image is not None:
                metadata[id]["link_card_draft_image"] = \
                  get_unfurl_image(person_id, image)

    
    for key, identity in identities.items():
        target = models.delivery_target.upsert({
            "person_id": person_id,
            "delivery_id": delivery["id"],
            "identity_id": key,
            "state": "pending"
        })

        delivery["targets"].append(target["id"])
       
        models.task.add({
            "queue": identity["platform"],
            "name": "create post",
            "priority": 1,
            "details": {
              "target": target,
              "identity": identity,
              "post": post,
              "metadata": metadata[identity["id"]],
            }
        })

    draft["state"] = "submitted"
    models.draft.update(draft["id"], draft)
    proof["state"] = "submitted"
    models.proof.update(proof["id"], proof)
    models.delivery.update(delivery["id"], delivery)
   
    return {
        "content": models.delivery.fetch(delivery["id"])
    }