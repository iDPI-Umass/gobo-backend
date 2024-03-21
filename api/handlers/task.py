import logging
from flask import request
import http_errors
import models
from .helpers import parse_query

def tasks_post():
    return {"content": models.task.add(request.json)}


def tasks_get():
    views = ["created"]
    parameters = parse_query(views, request.args)
    return {"content": models.task.query(parameters)}

def task_get(id):
    task = models.task.get(id)
    if task == None:
        raise http_errors.not_found(f"task {id} is not found")
    
    return {"content": task}

def task_put(id):
    if request.json["id"] != None and id != request.json["id"]:
        raise http_errors.unprocessable_content(
            f"task {id} does not match resource in body, rejecting"
        )

    task = models.task.update(id, request.json)
    if task == None:
        raise http_errors.unprocessable_content(
            f"task {id} is not found, create using tasks post"
        )
    else:
        return {"content": task}

def task_delete(id):
    task = models.task.remove(id)
    if task == None:
        raise http_errors.not_found(f"task {id} is not found")

    return {"content": ""}