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
        draft = models.draft_file.find({
            "id": id,
            "person_id": person_id
        })
        if draft is None:
            raise http_errors.bad_request(
                f"bluesky link unfurl image upload {person_id}/{id} is not found"
            )
        else:
            return draft


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
    if delivery["draft_id"] != request.json["draft_id"]:
        raise http_errors.bad_request(
            f"draft id does not match the id listed in the delivery"
        )
    if delivery["proof_id"] != request.json["proof_id"]:
        raise http_errors.bad_request(
            f"proof id does not match the id listed in the delivery"
        )
    

    draft_id = request.json["draft_id"]
    draft = models.draft.find({
        "person_id": person_id,
        "id": draft_id
    })
    if draft is None:
        raise http_errors.bad_request(
            f"person {person_id} does not have draft {id}"
        )
    

    proof_id = request.json["proof_id"]
    proof = models.proof.find({
        "person_id": person_id,
        "id": proof_id
    })
    if proof is None:
        raise http_errors.bad_request(
            f"person {person_id} does not have draft {id}"
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
        if identity["person_id"] != person_id:
            raise http_errors.bad_request(
                f"cannot publish to identity {id}"
            )
    

    # Establish post data core.
    post = {
        "title": draft.get("title"),
        "content": draft.get("content"),
        "poll": draft.get("poll")
    }


    # Confirm attachments have been uploaded already.
    file_ids = draft.get("files", [])
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
        delivery_target = models.delivery_target.upsert({
            "person_id": person_id,
            "delivery_id": delivery["id"],
            "identity_id": key,
            "state": "pending"
        })

        delivery["targets"].append(delivery_target["id"])
       
        models.task.add({
            "queue": identity["platform"],
            "name": "create post",
            "priority": 1,
            "details": {
              "delivery_target": delivery_target,
              "identity": identity,
              "post": post,
              "metadata": metadata[identity["id"]],
            }
        })

    models.delivery.update(delivery["id"], delivery)


    proof["state"] = "submitted"
    models.proof.update(proof["id"], proof)
   
    return {
        "content": models.delivery.fetch(delivery["id"])
    }