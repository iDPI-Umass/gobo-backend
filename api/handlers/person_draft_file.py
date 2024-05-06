import logging
import os
import mimetypes
from flask import request
import http_errors
import models
import joy


def person_draft_files_post(person_id):
    # Check to make sure we're not being flooded with draft files.
    drafts = models.draft_file.pull([
        models.helpers.where("person_id", person_id),
        models.helpers.where("published", False)
    ])

    if len(drafts) > 100:
        raise http_errors.unprocessable_content(
            f"person {person_id} has reached the maximum number of draft files."
        )
    

    # Pull out file data.    
    if "file" not in request.files:
        raise http_errors.bad_request("must include file in upload")
    
    file = request.files["file"]
    name = request.form.get("name", "")
    mime_type = request.form.get("mime_type")
    alt = request.form.get("alt")
    id = joy.crypto.address()  
    
    # Includes support for implicit MIME type resolution.
    if mime_type is None:
      mime_type, encoding = mimetypes.guess_type(name)
    if mime_type is None:
        raise http_errors.bad_request("unable to determine MIME type of file upload")
  
    
    # Add file to drive
    filepath = os.path.join(os.environ.get("UPLOAD_DIRECTORY"), id)
    file.save(filepath)

    # Add file metadata to db
    draft = models.draft_file.add({
        "id": id,
        "person_id": person_id,
        "name": name,
        "alt": alt,
        "published": False,
        "mime_type": mime_type
    })
    
    return {"content": draft}

def person_draft_file_delete(person_id, id):
    # Locate the draft file
    draft = models.draft_file.find({
        "person_id": person_id,
        "id": id
    })

    if draft is None:
        raise http_errors.not_found(
            f"draft file {person_id} / {id} is not found"
        )
    

    # Delete file from drive
    name = os.path.join(os.environ.get("UPLOAD_DIRECTORY"), id)
    if os.path.exists(name):
        os.remove(name)
    else:
        logging.warning(f"The draft file {id} is not present in the upload directory")

    # Delete file metadata from db
    models.draft_file.remove(id)

    # 204 Response
    return {"content": ""}