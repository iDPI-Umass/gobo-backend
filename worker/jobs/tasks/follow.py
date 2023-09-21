import logging
import joy
import models
import queues

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def follow(task):
    identity_id = task.details.get("identity_id")
    source_id = task.details["source_id"]
    if identity_id is None or source_id is None:
        raise Exception("follow requires source and identity IDs")

    models.link.upsert({
        "origin_type": "identity",
        "origin_id": identity_id,
        "target_type": "source",
        "target_id": source_id,
        "name": "follows",
        "secondary": None
    })

    links = QueryIterator(
        model = models.link,
        wheres = [
            where("origin_type", "source"),
            where("origin_id", source_id),
            where("target_type", "post"),
            where("name", "has-post")
        ]
    )

    for link in links:
        models.link.upsert({
            "origin_type": "identity",
            "origin_id": identity_id,
            "target_type": "post",
            "target_id": link["target_id"],
            "name": "identity-feed",
            "secondary": link["secondary"]
        })

def unfollow(task):
    identity_id = task.details.get("identity_id")
    source_id = task.details.get("source_id")
    if identity_id is None or source_id is None:
        raise Exception("unfollow requires source and identity IDs")

    models.link.find_and_remove({
        "origin_type": "identity",
        "origin_id": identity_id,
        "target_type": "source",
        "target_id": source_id,
        "name": "follows"       
    })

    links = QueryIterator(
        model = models.link,
        wheres = [
            where("origin_type", "identity"),
            where("origin_id", identity_id),
            where("target_type", "post"),
            where("name", "identity-feed")
        ]
    )
   
    for link in links:
        post = models.post.get(link["target_id"])
        if post is not None and post["source_id"] == source_id:
            models.link.remove(link["id"])


def remove_identity(task):
    identity_id = task.details.get("identity_id")
    if identity_id is None:
        raise Exception("remove identity requires identity_id")

    links = QueryIterator(
        model = models.link,
        wheres = [
            where("origin_type", "identity"),
            where("origin_id", identity_id),
            where("target_type", "source"),
            where("name", "follows")
        ]
    )
    for link in links:
        models.link.remove(link["id"])

    
    links = QueryIterator(
        model = models.link,
        wheres = [
            where("origin_type", "identity"),
            where("origin_id", identity_id),
            where("target_type", "post"),
            where("name", "identity-feed")
        ]
    )
    for link in links:
        models.link.remove(link["id"])