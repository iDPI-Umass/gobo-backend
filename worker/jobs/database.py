import logging
import models
import queues

where = models.helpers.where


def dispatch(task):
    task.handler = "database"
    logging.info("dispatching: %s", task)

    if task.name == "add post to followers":
        add_post_to_followers(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


def add_post_to_followers(task):
    page = task.details.get("page") or 1
    per_page = task.details.get("per_page") or 500
    post = task.details.get("post")
    if post == None:
        raise Exception("add posts to followers must have post defined, bailing")

    followers = models.link.query({
        "page": page,
        "per_page": per_page,
        "where": [
            where("origin_type", "person"),
            where("target_type", "source"),
            where("target_id", post["source_id"]),
            where("name", "follows")
        ]
    })

    if len(followers) == per_page:
        task.update({"page": page + 1})
        queues.database.put_task(task)

    for follower in followers:
        models.link.upsert({
            "origin_type": "person",
            "origin_id": follower["origin_id"],
            "target_type": "post",
            "target_id": post["id"],
            "name": "full-feed",
            "secondary": post["published"]
        })
