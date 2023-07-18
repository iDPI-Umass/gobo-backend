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
    elif task.name == "remove identity":
        remove_identity(task)
    elif task.name == "add post to followers":
        add_post_to_followers(task)
    elif task.name == "rebuild feed":
        rebuild_feed(task)
    elif task.name == "workbench":
        workbench(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


def follow(task):
    identity_id = task.details.get("identity_id")
    source_id = task.details["source_id"]
    if identity_id is None or source_id is None:
        raise Exception("follow requires source and identity IDs")

    models.link.safe_add({
        "origin_type": "identity",
        "origin_id": identity_id,
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
                "origin_type": "identity",
                "origin_id": identity_id,
                "target_type": "post",
                "target_id": link["target_id"],
                "name": "identity-feed",
                "secondary": link["secondary"]
            })

        if len(links) == per_page:
            query["page"] = query["page"] + 1
        else:
            break

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

    per_page = 1000
    while True:
        query = {
            "page": 1,
            "per_page": per_page,
            "where": [
                where("origin_type", "identity"),
                where("origin_id", identity_id),
                where("target_type", "post"),
                where("name", "identity-feed")
            ]
        }
        
        links = models.link.query(query)
        for link in links:
            models.link.remove(link["id"])

        if len(links) == per_page:
            query["page"] = query["page"] + 1
        else:
            break


def remove_identity(task):
    identity_id = task.details.get("identity_id")
    if identity_id is None:
        raise Exception("remove identity requires identity_id")

    per_page = 1000
    while True:
        query = {
            "page": 1,
            "per_page": per_page,
            "where": [
                where("origin_type", "identity"),
                where("origin_id", identity_id),
                where("target_type", "source"),
                where("name", "follows")
            ]
        }
        
        links = models.link.query(query)
        for link in links:
            models.link.remove(link["id"])

        if len(links) == per_page:
            query["page"] = query["page"] + 1
        else:
            break

    per_page = 1000
    while True:
        query = {
            "page": 1,
            "per_page": per_page,
            "where": [
                where("origin_type", "identity"),
                where("origin_id", identity_id),
                where("target_type", "post"),
                where("name", "identity-feed")
            ]
        }
        
        links = models.link.query(query)
        for link in links:
            models.link.remove(link["id"])

        if len(links) == per_page:
            query["page"] = query["page"] + 1
        else:
            break

def rebuild_feed(task):
    person_id = task.details.get("person_id")
    if person_id is None:
        raise Exception("rebuild feed requires person_id")

    per_page = 1000
    while True:
        identity_query = {
            "page": 1,
            "per_page": per_page,
            "where": [
                 where("origin_type", "person"),
                  where("origin_id", person_id),
                  where("target_type", "identity"),
                  where("name", "has-identity")
            ]
        }

        identities = models.link.query(identity_query)
        for identity in identities:
            identity_id = identity["target_id"]
            
            while True:
                source_query = {
                    "page": 1,
                    "per_page": per_page,
                    "where": [
                        where("origin_type", "identity"),
                        where("origin_id", identity_id),
                        where("target_type", "source"),
                        where("name", "follows")
                    ]
                }

                sources = models.link.query(source_query)
                for source in sources:
                    source_id = source["target_id"]
                    queues.database.put_details("follow", {
                        "identity_id": identity_id,
                        "source_id": source_id
                    })

                if len(sources) == per_page:
                    source_query["page"] = source_query["page"] + 1
                else:
                    break


        if len(identities) == per_page:
            identity_query["page"] = identity_query["page"] + 1
        else:
            break




def add_post_to_followers(task):
    page = task.details.get("page") or 1
    per_page = task.details.get("per_page") or 500
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
        queues.database.put_task(task)

    for follower in followers:
        models.link.upsert({
            "origin_type": "identity",
            "origin_id": follower["origin_id"],
            "target_type": "post",
            "target_id": post["id"],
            "name": "identity-feed",
            "secondary": f"{post['published']}::{post['id']}"
        })


def workbench(task):    
    links = models.link.pull([])
    keys = set()
    for link in links:
        a = link["origin_type"]
        b = link["origin_id"]
        c = link["target_type"]
        d = link["target_id"]
        e = link["name"]
        key = f"{a}{b}{c}{d}{e}"
        if key in keys:
            logging.warning(key)
            # models.link.remove(link["id"])
        else:
            keys.add(key)