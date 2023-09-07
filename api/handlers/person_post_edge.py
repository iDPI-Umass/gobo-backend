import logging
from flask import request
import http_errors
import models
from .helpers import resolve_platform


def check_identity(person_id, id):
    identity = models.identity.find({
        "id": id,
        "person_id": person_id
    })
    if identity is None:
        raise http_errors.forbidden(f"cannot view or update edges for identity {id}")
    return identity


def check_post(base_url, id):
    post = models.post.get(id)
    if post is None:
        raise http_errors.not_found(f"post {id} is not found")
    if post["base_url"] != base_url:
        raise http_errors.unprocessable_content(
            f"this identity cannot view or update edges for post {id}"
        )
    return post


def edge_not_found(person_id, id):
    raise http_errors.not_found(f"post edge {person_id}/{id} is not found")



def person_post_edges_post(person_id):
    identity = request.json["identity"]
    post = request.json["post"]
    name = request.json["name"]

    identity = check_identity(person_id, identity)
    post = check_post(identity["base_url"], post)

    post_edge = models.post_edge.find({
        "identity_id": identity["id"],
        "post_id": post["id"],
        "name": name
    })

    # We'll allow this operation to be idempotent.
    if post_edge is not None:
        return post_edge

    post_edge = models.post_edge.add({
        "identity_id": identity["id"],
        "post_id": post["id"],
        "name": name
    })

    models.task.add({
        "queue": resolve_platform(identity["base_url"]),
        "name": "add post edge",
        "details": {
            "identity": identity,
            "post": post,
            "name": name,
            "edge": post_edge
        }
    })

    return post_edge


def person_post_edge_get(person_id, id):
    edge = models.post_edge.get(id)
    if edge is None:
        edge_not_found(person_id, id)
    
    identity = models.identity.find({
        "person_id": person_id,
        "id": edge["identity_id"]
    })
    if identity is None:
        edge_not_found(person_id, id)
        
    return edge


def person_post_edge_delete(person_id, id):
    edge = models.post_edge.get(id)
    if edge is None:
        edge_not_found(person_id, id)
    
    identity = models.identity.find({
        "person_id": person_id,
        "id": edge["identity_id"]
    })
    if identity is None:
        edge_not_found(person_id, id)

    post = models.post.get(edge["post_id"])
    if post is None:
        raise http_errors.not_found(f"post {id} is not found")

    models.post_edge.remove(id)
    models.task.add({
        "queue": resolve_platform(identity["base_url"]),
        "name": "remove post edge",
        "details": {
            "identity": identity,
            "post": post,
            "name": edge["name"],
            "edge": edge
        }
    })
    
    return ""