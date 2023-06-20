import logging
import models
import queues

where = models.helpers.where


def dispatch(task):
    task.handler = "database"
    logging.info("dispatching: %s", task)

    if task.name == "follow":
        follow(task)
    elif task.name == "unfollow":
        unfollow(task)
    elif task.name == "add post to followers":
        add_post_to_followers(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


def follow(task):
    person_id = task.details.get("person_id")
    source_id = task.details.get("source_id")
    if person_id == None or source_id == None:
        raise Exception("follow requires source and person IDs")

    models.link.safe_add({
        "origin_type": "person",
        "origin_id": person_id,
        "target_type": "source",
        "target_id": source_id,
        "name": "follows",
        "secondary": None
    })

    per_page = 1000
    while True:
        query = {
            "page": 1,
            "per_page": per_page,
            "where": [
                where("origin_type", "source"),
                where("origin_id", source_id),
                where("target_type", "post"),
                where("name", "has-post")
            ]
        }
        
        links = models.link.query(query)
        for link in links:
            models.link.upsert({
                "origin_type": "person",
                "origin_id": person_id,
                "target_type": "post",
                "target_id": link["target_id"],
                "name": "full-feed",
                "secondary": link["secondary"]
            })

        if len(links) == per_page:
            query["page"] = query["page"] + 1
        else:
            break

def unfollow(task):
    person_id = task.details.get("person_id")
    source_id = task.details.get("source_id")
    if person_id == None or source_id == None:
        raise Exception("unfollow requires source and person IDs")

    models.link.find_and_remove({
        "origin_type": "person",
        "origin_id": person_id,
        "target_type": "source",
        "target_id": source_id,
        "name": "follows"       
    })

    per_page = 1000
    while True:
        query = {
            "page": 1,
            "per_page": per_page,
            "where": [
                where("origin_type", "source"),
                where("origin_id", source_id),
                where("target_type", "post"),
                where("name", "has-post")
            ]
        }
        
        links = models.link.query(query)
        for link in links:
            models.link.find_and_remove({
                "origin_type": "person",
                "origin_id": person_id,
                "target_type": "post",
                "target_id": link["target_id"],
                "name": "full-feed"
            })

        if len(links) == per_page:
            query["page"] = query["page"] + 1
        else:
            break


def add_post_to_followers(task):
    page = task.details.get("page") or 1
    per_page = task.details.get("per_page") or 500
    post = task.details.get("post")
    if post == None:
        raise Exception("add posts to followers must have post defined")

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
