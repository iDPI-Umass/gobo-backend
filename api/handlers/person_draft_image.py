import logging
import os
import imghdr
from flask import request
import http_errors
import models
import joy

allowed_image_types = [
  "jpeg",
  "png",
  "webp"
]


def person_draft_images_post(person_id):
    # Check to make sure we're not being flooded with draft images.
    drafts = models.draft_image.pull([
        models.helpers.where("person_id", person_id),
        models.helpers.where("published", False)
    ])

    if len(drafts) > 50:
        raise http_errors.unprocessable_content(
            f"person {person_id} has reached the maximum number of draft images."
        )
    

    # Pull out image data.    
    if "image" not in request.files:
        raise http_errors.bad_request("must include image in upload")
    
    image = request.files["image"]
    name = request.form.get("name", None)
    alt = request.form.get("alt", None)
    id = joy.crypto.random({"encoding": "base32"})


    # Check image MIME type internally
    mime_type = imghdr.what(image)
    if mime_type not in allowed_image_types:
        raise http_errors.bad_request(
            f"GOBO does not accept {mime_type} files through this endpoint"
        )
    
    # Add image to drive
    filepath = os.path.join(os.environ.get("UPLOAD_DIRECTORY"), id)
    image.save(filepath)

    # Add image metadata to db
    draft = models.draft_image.add({
        "id": id,
        "person_id": person_id,
        "name": name,
        "alt": alt,
        "published": False,
        "mime_type": mime_type
    })
    
    return {"content": draft}

def person_draft_image_delete(person_id, id):
    # Locate the draft image
    draft = models.draft_image.find({
        "person_id": person_id,
        "id": id
    })

    if draft is None:
        raise http_errors.not_found(
            f"draft image {person_id} / {id} is not found"
        )
    

    # Delete image from drive
    name = os.path.join(os.environ.get("UPLOAD_DIRECTORY"), id)
    if os.path.exists(name):
        os.remove(name)
    else:
        logging.warning(f"The draft image {id} is not present in the upload directory")

    # Delete image metadata from db
    models.draft_image.remove(id)

    # 204 Response
    return {"content": ""}