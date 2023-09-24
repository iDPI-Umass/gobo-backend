import logging
from flask import request
import http_errors
import models


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

allowed_names = ["like", "repost", "upvote", "downvote"]
def check_name(name):
    if name not in allowed_names:
        raise http_errors.bad_request(f"name {name} is not a recognized edge")




def person_post_edge_put(person_id, identity_id, post_id, name):
    identity = check_identity(person_id, identity_id)
    post = check_post(identity["base_url"], post_id)
    check_name(name)

    post_edge = models.post_edge.find({
        "identity_id": identity_id,
        "post_id": post_id,
        "name": name
    })

    # PUTs are idempotent.
    if post_edge is not None:
        return post_edge

    post_edge = models.post_edge.add({
        "identity_id": identity["id"],
        "post_id": post["id"],
        "name": name
    })

    models.task.add({
        "queue": identity["platform"],
        "name": "add post edge",
        "details": {
            "identity": identity,
            "post": post,
            "name": name,
            "edge": post_edge
        }
    })

    return post_edge



def person_post_edge_delete(person_id, identity_id, post_id, name):
    edge = models.post_edge.find({
        "identity_id": identity_id,
        "post_id": post_id,
        "name": name
    })
    if edge is None:
        raise http_errors.not_found(f"post edge {name} is not found")
    
    identity = models.identity.find({
        "person_id": person_id,
        "id": identity_id
    })
    if identity is None:
        raise http_errors.not_found(f"post edge {name} is not found")

    post = models.post.get(post_id)
    if post is None:
        raise http_errors.not_found(f"post edge {name} is not found")

    models.post_edge.remove(edge["id"])
    models.task.add({
        "queue": identity["platform"],
        "name": "remove post edge",
        "details": {
            "identity": identity,
            "post": post,
            "name": name,
            "edge": edge
        }
    })
    
    return ""