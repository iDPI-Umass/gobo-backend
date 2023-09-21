import logging
import joy
import models
import queues
from .helpers import is_valid_platform

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def hard_reset(task):
    queues.default.put_details("remove posts", task.details)
    queues.default.put_details("reset last retrieved", task.details)


def clear_posts(task):
    page = task.details.get("page") or 1
    per_page = 1000
    platform = task.details.get("platform", None)
    if not is_valid_platform(platform):
        raise Exception(f"clear posts does not support platform {platform}")
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]
        
   
    posts = models.post.query({
        "page": page,
        "per_page": per_page,
        "where": wheres
    })

    if len(posts) == per_page:
        task.update({"page": page + 1})
        queues.default.put_task(task)

    for post in posts:
        id = post["id"]

        
        links = QueryIterator(
            model = models.link,
            wheres = [
                where("origin_type", "post"),
                where("origin_id", id)
            ]
        )
        for link in links:
            models.link.remove(link["id"])

        
        links = QueryIterator(
            model = models.link,
            wheres = [
                where("target_type", "post"),
                where("target_id", id)
            ]
        )
        for link in links:
            models.link.remove(link["id"])

        models.post.remove(id)


def clear_last_retrieved(task):
    page = task.details.get("page") or 1
    per_page = 1000
    platform = task.details.get("platform", None)
    if not is_valid_platform(platform):
        raise Exception(f"clear posts does not support platform {platform}")
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]

    sources = models.source.query({
        "page": page,
        "per_page": per_page,
        "where": wheres
    })

    for source in sources:
        link = models.link.find({
            "origin_type": "source",
            "origin_id": source["id"],
            "name": "last-retrieved"
        })
        if link is not None:
            models.link.remove(link["id"])

    if len(sources) == per_page:
        task.update({"page": page + 1})
        queues.default.put_task(task)