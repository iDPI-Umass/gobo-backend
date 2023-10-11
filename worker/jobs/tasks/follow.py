import logging
import joy
import models
import queues
from . import helpers as h

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def follow(task):
    identity_id = h.enforce("identity_id", task)
    source_id = h.enforce("source_id", task)

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
    identity_id = h.enforce("identity_id", task)
    source_id = h.enforce("source_id", task)
    
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
            where("origin_type", "source"),
            where("origin_id", source_id),
            where("target_type", "post"),
            where("name", "has-post")
        ]
    )
   
    for link in links:
        models.link.find_and_remove({
            "origin_type": "identity",
            "origin_id": identity_id,
            "target_type": "post",
            "target_id": link["target_id"],
            "name": "identity-feed",
            "secondary": link["secondary"]
        })


def rebuild_feed(task):
    person_id = h.enforce("person_id", task)

    identities = QueryIterator(
        model = models.link,
        wheres = [
            where("origin_type", "person"),
            where("origin_id", person_id),
            where("target_type", "identity"),
            where("name", "has-identity")
        ]
    )

    for identity in identities:
        identity_id = identity["target_id"]
            
        sources = QueryIterator(
            model = models.link,
            wheres = [
                where("origin_type", "identity"),
                where("origin_id", identity_id),
                where("target_type", "source"),
                where("name", "follows")
            ]
        )

        for source in sources:
            source_id = source["target_id"]
            queues.default.put_details("follow", {
                "identity_id": identity_id,
                "source_id": source_id
            })


def remove_identity(task):
    identity_id = h.enforce("identity_id", task)

    links = QueryIterator(
        model = models.link,
        for_removal = True,
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
        for_removal = True,
        wheres = [
            where("origin_type", "identity"),
            where("origin_id", identity_id),
            where("target_type", "post"),
            where("name", "identity-feed")
        ]
    )
    for link in links:
        models.link.remove(link["id"])