import logging
from flask import request
import http_errors
import models
from platform_models import bluesky, reddit


def person_posts_post(person_id):
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
        

    post = request.json["post"]
    attachment_ids = post.get("attachments", [])
    attachments = []
    for id in attachment_ids:
        draft = models.draft_image.find({
            "id": id,
            "person_id": person_id
        })

        if draft is None:
            raise http_errors.bad_request(
                f"draft image {person_id}/{id} is not found"
            )
       
        attachments.append(draft)

    post["attachments"] = attachments


    tasks = []
    for key, identity in identities.items():
        base_url = identity["base_url"]
        if base_url == bluesky.BASE_URL:
            queue = "bluesky"
        elif base_url == reddit.BASE_URL:
            queue = "reddit"
        else:
            queue = "mastodon"

        tasks.append(models.task.add({
            "queue": queue,
            "name": "create post",
            "details": {
              "identity": identity,
              "post": post,
              "metadata": metadata[identity["id"]],
            }
        }))


    return tasks