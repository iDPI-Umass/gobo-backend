import logging
import os
import mimetypes
from flask import request
import http_errors
import models
import joy

def check_draft_data(person_id, data):
    if data["person_id"] != person_id:
        raise http_errors.bad_request("draft person_id does not match resource")
    for attachment in data["attachments"]:
        file = models.draft_file.get(attachment)
        if file is None or file["person_id"] != person_id:
            raise http_errors.not_found(
                f"draft file {person_id} / {attachment} is not found"
            )
    if data.get("state") is None:
        data["state"] = "draft"



def person_drafts_post(person_id): 
    data = dict(request.json)
    check_draft_data(person_id, data)
    draft = models.draft.add(data)
    return {"content": draft}


def person_draft_put(person_id, id):
    draft = models.draft.find({
        "person_id": person_id,
        "id": id
    })
    if draft is None:
        raise http_errors.not_found(f"draft {person_id} / {id} is not found")
    
    data = dict(request.json)
    check_draft_data(person_id, data)
    draft = models.draft.update(id, data) 
    return {"content": draft}

def person_draft_delete(person_id, id):
    # Locate the draft file
    draft = models.draft.find({
        "person_id": person_id,
        "id": id
    })

    if draft is None:
        raise http_errors.not_found(
            f"draft file {person_id} / {id} is not found"
        )
    
    for attachment in draft["attachments"]:
        # Delete file from drive
        name = os.path.join(os.environ.get("UPLOAD_DIRECTORY"), attachment)
        if os.path.exists(name):
            os.remove(name)
        else:
            logging.warning(f"The draft file {id} is not present in the upload directory")

        # Delete file metadata from db
        models.draft_file.remove(attachment)

    # Ready to remove draft resource
    models.draft.remove(id)

    # 204 Response
    return {"content": ""}