import logging
from flask import request
import http_errors
import models
from .helpers import parse_query, parse_base_url


def mastodon_clients_post():
    parse_base_url(request.json)
    base_url = request.json["base_url"]
    client = models.mastodon_client.find({"base_url": base_url})
    if client != None:
        raise http_errors.conflict(f"mastodon_client {base_url} is already registered")

    return {"content": models.mastodon_client.add(request.json)}

def mastodon_clients_get():
    views = ["created", "base_url"]
    parameters = parse_query(views, request.args)
    return {"content": models.mastodon_client.query(parameters)}

def mastodon_client_get(id):
    client = models.mastodon_client.get(id)
    if client == None:
        raise http_errors.not_found(f"mastodon_client {id} is not found")
    
    return {"content": client}

def mastodon_client_put(id):
    parse_base_url(request.json)
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"mastodon_client {id} does not match resource in body, rejecting"
        )

    client = models.mastodon_client.update(id, request.json)
    if client == None:
        raise http_errors.unprocessable_content(
            f"mastodon_client {id} is not found, create using people post"
        )
    else:
        return {"content": client}

def mastodon_client_delete(id):
    client = models.mastodon_client.remove(id)
    if client == None:
        raise http_errors.not_found(f"mastodon_client {id} is not found")

    return {"content": ""}