import logging
import joy
import models
import queues
import helpers as h

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def add_post_to_followers(task):
    page = task.details.get("page") or 1
    per_page = task.details.get("per_page") or 1000
    post = h.enforce("post", task)

    followers = models.link.query({
        "page": page,
        "per_page": per_page,
        "where": [
            where("origin_type", "identity"),
            where("target_type", "source"),
            where("target_id", post["source_id"]),
            where("name", "follows")
        ]
    })

    if len(followers) == per_page:
        task.update({"page": page + 1})
        queues.default.put_task(task)

    for follower in followers:
        models.link.upsert({
            "origin_type": "identity",
            "origin_id": follower["origin_id"],
            "target_type": "post",
            "target_id": post["id"],
            "name": "identity-feed",
            "secondary": f"{post['published']}::{post['id']}"
        })


def remove_post(task):
    post = h.enforce("post", task)

    links = QueryIterator(
        model = models.link,
        wheres = [
            where("origin_type", "post"),
            where("origin_id", post["id"])
        ]
    )
    for link in links:
        models.link.remove(link["id"])

    links = QueryIterator(
        model = models.link,
        wheres = [
            where("target_type", "post"),
            where("target_id", post["id"])
        ]
    )
    for link in links:
        models.link.remove(link["id"])

    models.post.remove(post["id"])

    

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