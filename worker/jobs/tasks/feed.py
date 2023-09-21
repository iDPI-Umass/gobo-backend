import logging
import joy
import models
import queues

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def add_post_to_source(task):
    post = task.details.get("post")
    if post is None:
        raise Exception("add post to source: needs post")

    post = models.post.upsert(post)
    models.link.upsert({
        "origin_type": "source",
        "origin_id": post["source_id"],
        "target_type": "post",
        "target_id": post["id"],
        "name": "has-post",
        "secondary": f"{post['published']}::{post['id']}"
    })

    queues.default.put_details("add post to followers", {
        "page": 1,
        "post": post
    })

# In cases where we see a post, but may not have the full picture, use
# this safe_add method to avoid overwriting the full post.
def add_partial_post(task):
    post = task.details.get("post")
    if post is None:
        raise Exception("add partial post: needs post")

    post = models.post.safe_add(post)
    models.link.upsert({
        "origin_type": "source",
        "origin_id": post["source_id"],
        "target_type": "post",
        "target_id": post["id"],
        "name": "has-post",
        "secondary": f"{post['published']}::{post['id']}"
    })

def add_interpost_edge(task):
    base_url = task.details.get("base_url")
    data = task.details.get("edge_reference")
    if base_url is None:
        raise Exception("add interpost edge: need to specify base_url both posts belong to.")
    if data is None:
        raise Exception("add interpost edge: needs edge primitive")


    origin = models.post.find({
        "base_url": base_url,
        "platform_id": data["origin_reference"]
    })
    if origin is None:
        raise Exception("add interpost edge: origin post is not available in post table")

    target = models.post.find({
        "base_url": base_url,
        "platform_id": data["target_reference"]
    })
    if target is None:
        raise Exception(f"target post is not available in post table {task.details}")


    models.link.upsert({
        "origin_type": "post",
        "origin_id": origin["id"],
        "target_type": "post",
        "target_id": target["id"],
        "name": data["name"],
        "secondary": f"{target['published']}::{target['id']}"
    })

def add_post_to_followers(task):
    page = task.details.get("page") or 1
    per_page = task.details.get("per_page") or 1000
    post = task.details.get("post")
    if post is None:
        raise Exception("add posts to followers must have post defined")

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
    post = task.details.get("post")
    if post is None:
        raise Exception("remove post requires post")

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
    person_id = task.details.get("person_id")
    if person_id is None:
        raise Exception("rebuild feed requires person_id")

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