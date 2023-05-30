import logging
from flask import request
import models
from .helpers import parse_query

def links_post():
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