import logging
import os
import mimetypes
from flask import request
import http_errors
import models
import joy

def person_draft_files_post(person_id):
    # Check to make sure we're not being flooded with draft files.
    files = models.draft_file.pull([
        models.helpers.where("person_id", person_id),
        models.helpers.where("published", False)
    ])

    if len(files) > 1000:
        raise http_errors.unprocessable_content(
            f"person {person_id} has reached the maximum number of draft files."
        )
    
    # create file metadata slot in database and come up with its ID.
    file = models.draft_file.add({
        "id": id,
        "person_id": person_id,
        "published": False,
        "state": "pending"
    })
    
    return {"content": file}


def person_draft_file_post(person_id, id):
    # Locate the draft file slot
    draft = models.draft_file.find({
        "person_id": person_id,
        "id": id
    })

    if draft is None:
        raise http_errors.not_found(
            f"draft file {person_id} / {id} is not found"
        )
    

    # Pull out file data.    
    if "file" not in request.files:
        raise http_errors.bad_request("must include file in upload")
    
    file = request.files["file"]
    name = request.form.get("name", "")
    mime_type = request.form.get("mime_type")
    alt = request.form.get("alt")
    id = joy.crypto.address()  
    
    # Support for implicit MIME type resolution as last resort.
    if mime_type is None:
      mime_type, encoding = mimetypes.guess_type(name)
    if mime_type is None:
        raise http_errors.bad_request("unable to determine MIME type of file upload")
  
    # TODO: This is not good, but we need to present praw with a filename that
    #  indicates the MIME type because they don't allow explicit configurationn there.
    id = id + mimetypes.guess_extension(mime_type)
    
    # Add file to drive
    filepath = os.path.join(os.environ.get("UPLOAD_DIRECTORY"), id)
    file.save(filepath)

    # Add file metadata to db
    draft = models.draft_file.update(id, {
        "id": id,
        "person_id": person_id,
        "name": name,
        "alt": alt,
        "published": False,
        "mime_type": mime_type,
        "state": "uploaded"
    })
    
    return {"content": draft}

def person_draft_file_put(person_id, id):
    # Locate the draft file
    file = models.draft_file.find({
        "person_id": person_id,
        "id": id
    })
    if file is None:
        raise http_errors.not_found(
            f"draft file {person_id} / {id} is not found"
        )
    
    file = dict(request.json)
    if file["person_id"] != person_id:
         raise http_errors.bad_request(
            "person_id of body does not match resource path"
        )       

    # Update file metadata in db
    file = models.draft_file.update(id, file)
    return {"content": file}


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