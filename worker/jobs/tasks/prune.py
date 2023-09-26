import logging
import os
from os import environ
from datetime import timedelta
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def prune_resources(task):
    queues.default.put_details("prune draft images")
    queues.default.put_details("prune posts")
    queues.default.put_details("prune registrations")
    queues.default.put_details("prune sources")



def prune_draft_images(task):
    oldest_limit = joy.time.convert("date", "iso", 
        joy.time.nowdate() - timedelta(hours=12)
    )

    drafts = QueryIterator(
        model = models.draft_image,
        for_removal = True,
        wheres = [
            where("created", oldest_limit , "lt")
        ]
    )

    for draft in drafts:
        filename = os.path.join(
            environ.get("UPLOAD_DIRECTORY"), 
            draft["id"]
        )
        
        if os.path.exists(filename):
            os.remove(filename)
        
        models.draft_image.remove(draft["id"])



def prune_posts(task):
    oldest_limit = joy.time.convert("date", "iso", 
        joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
    )

    posts = QueryIterator(
        model = models.post,
        for_removal = True,
        wheres = [
            where("created", oldest_limit, "lt")
        ]
    )
    for post in posts:
        h.remove_post(post)



def prune_registrations(task):
    oldest_limit = joy.time.convert("date", "iso", 
        joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
    )

    registrations = QueryIterator(
        model = models.registration,
        for_removal = True,
        wheres = [
            where("created", oldest_limit, "lt")
        ]
    )
    for registration in registrations:
        models.registration.remove(registration["id"])



def prune_sources(task):
    oldest_limit = joy.time.convert("date", "iso", 
        joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
    )

    # NOTE: This depends on the fact that we upsert sources opportunistically
    #       when they cross our path. So if updated is older than two weeks,
    #       no one is following it and there is no valid associated post.
    sources = QueryIterator(
        model = models.source,
        for_removal = True,
        wheres = [
            where("updated", oldest_limit, "lt")
        ]
    )

    for source in sources:
        h.remove_source(source)