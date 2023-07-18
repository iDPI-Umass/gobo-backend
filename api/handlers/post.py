import logging
from flask import request
import http_errors
import models
from .helpers import parse_query


def posts_post():
    url = request.json["url"]
    post = models.post.find({"url": url})
    if post != None:
        raise http_errors.conflict(f"post {url} is already registered")

    return models.post.add(request.json)

def posts_get():
    views = ["created"]
    parameters = parse_query(views, request.args)
    return models.post.query(parameters)

def post_get(id):
    post = models.post.get(id)
    if post == None:
        raise http_errors.not_found(f"post {id} is not found")
    
    return post

def post_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"post {id} does not match repost in body, rejecting"
        )

    post = models.post.update(id, request.json)
    if post == None:
        raise http_errors.unprocessable_content(
            f"post {id} is not found, create using people post"
        )
    else:
        return post

def post_delete(id):
    post = models.post.remove(id)
    if post == None:
        raise http_errors.not_found(f"post {id} is not found")

    return ""

def post_graph_get(id):
    result = models.post.view_post_graph(id)
    if len(result["feed"]) == 0:
        raise http_errors.not_found(f"post {id} is not found")
    
    return result