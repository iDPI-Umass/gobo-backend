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
    delivery_id = request.json["delivery"]
    delivery = models.delivery.fetch(delivery_id)
    if delivery is None:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )
    if delivery["person_id"] != person_id:
        raise http_errors.not_found(
            f"person {person_id} does not have delivery {id}"
        )
    
    metadata = {}
    identity_ids = []
    for target in request.json["targets"]:
        metadata[target["identity"]] = target.get("metadata", {})
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
            raise http_errors.not_found(
                f"cannot publish to identity {id}"
            )
        

    # Confirm attachments have been uploaded already.
    post = request.json["post"]
    attachment_ids = post.get("attachments", [])
    attachments = []
    for id in attachment_ids:
        draft = models.draft_file.find({
            "id": id,
            "person_id": person_id
        })

        if draft is None:
            raise http_errors.bad_request(
                f"draft image {person_id}/{id} is not found"
            )
       
        attachments.append(draft)

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
        models.delivery.update(delivery["id"], key, {
            "state": "pending"
        })
       
        models.task.add({
            "queue": identity["platform"],
            "name": "create post",
            "priority": 1,
            "details": {
              "delivery": delivery,
              "identity": identity,
              "post": post,
              "metadata": metadata[identity["id"]],
            }
        })


    return {
        "content": models.delivery.fetch(delivery["id"])
    }