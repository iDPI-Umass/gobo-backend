import logging
from flask import request
import http_errors
import models
from .helpers import parse_query

def links_post():
    link = models.link.find({
      "origin_type": request.json["origin_type"],
      "origin_id": request.json["origin_id"],
      "target_type": request.json["target_type"],
      "target_id": request.json["target_id"],
      "name": request.json["name"]
    })
    if link != None:
        raise http_errors.conflict("this link already exists")

    return models.link.add(request.json)

def _parse_link_query(parameters, args):
    value = args.get("origin_type")
    if value != None:
        parameters["where"].append({
            "key": "origin_type",
            "value": value,
            "operator": "eq"
        })
    
    value = args.get("target_type")
    if value != None:
        parameters["where"].append({
            "key": "target_type",
            "value": value,
            "operator": "eq"
        })

    value = args.get("name")
    if value != None:
        parameters["where"].append({
            "key": "name",
            "value": value,
            "operator": "eq"
        })

    value = args.get("origin_id")
    try:
        if value != None:
            parameters["where"].append({
                "key": "origin_id",
                "value": int(value),
                "operator": "eq"
            })
    except Exception as e:
        raise http_errors.bad_request(f"origin_id {value} is invalid")

    value = args.get("target_id")
    try:
        if value != None:
            parameters["where"].append({
                "key": "target_id",
                "value": int(value),
                "operator": "eq"
            })
    except Exception as e:
        raise http_errors.bad_request(f"target_id {value} is invalid")

def links_get():
    views = ["created"]
    parameters = parse_query(views, request.args)
    _parse_link_query(parameters, request.args)
    return models.link.query(parameters)

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