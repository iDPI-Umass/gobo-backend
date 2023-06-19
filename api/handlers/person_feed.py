import logging
from flask import request
import http_errors
import models
from .helpers import get_viewer, parse_feed_query


def person_feed_get(person_id):
    person = get_viewer(person_id)
    views = ["full"]
    query = parse_feed_query(views, request.args)
    query["person_id"] = person_id

    posts = models.post.view_feed(query)
    return posts