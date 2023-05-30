import logging
from flask import request
import http_errors
import models

def link_get(id):
    link = models.link.get(id)
    if link == None:
        raise http_errors.not_found(f"link {id} is not found")
    
    return link

def link_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"link {id} does not match resource in body, rejecting"
        )

    link = models.link.update(id, request.json)
    if link == None:
        raise http_errors.unprocessable_content(
            f"link {id} is not found, create using people post"
        )
    else:
        return link

def link_delete(id):
    link = models.link.remove(id)
    if link == None:
        raise http_errors.not_found(f"link {id} is not found")

    return ""