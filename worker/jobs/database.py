import logging
import html
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
    elif task.name == "add post to source":
        add_post_to_source(task)
    elif task.name == "add post to followers":
        add_post_to_followers(task)
    elif task.name == "add interpost edge":
        add_interpost_edge(task)
    elif task.name == "rebuild feed":
        rebuild_feed(task)
    elif task.name == "clean follows":
        clean_follows(task)
    elif task.name == "escape titles":
        escape_titles(task)
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

    models.link.upsert({
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

    queues.database.put_details("add post to followers", {
        "page": 1,
        "per_page": 500,
        "post": post
    })



def add_interpost_edge(task):
    base_url = task.details.get("base_url")
    data = task.details.get("edge_reference")
    if base_url is None:
        raise Exception("add shares to post: need to specify base_url both posts belong to.")
    if data is None:
        raise Exception("add shares to post: needs edge primitive")


    origin = models.post.find({
        "base_url": base_url,
        "platform_id": data["origin_reference"]
    })
    if origin is None:
        raise Exception("origin post is not available in post table")

    target = models.post.find({
        "base_url": base_url,
        "platform_id": data["target_reference"]
    })
    if target is None:
        raise Exception("target post is not available in post table")


    models.link.upsert({
        "origin_type": "post",
        "origin_id": origin["id"],
        "target_type": "post",
        "target_id": target["id"],
        "name": data["name"],
        "secondary": f"{target['published']}::{target['id']}"
    })

def escape_titles(task):
    query = {
        "page": 1,
        "per_page": 500,
        "where": []
    }

    while True:
        posts = models.post.query(query)
        for post in posts:
            title = post.get("title", None)
            if title is not None:
                post["title"] = html.unescape(title)
                models.post.update(post["id"], post)

        query["page"] += 1
        if len(posts) == 0:
            break



def reset_all_posts():
    posts = models.post.pull([])
    for post in posts:
        models.post.remove(post["id"])



    links = models.link.pull([
      where("origin_type", "post")
    ])
    for link in links:
       models.link.remove(link["id"])



    links = models.link.pull([
      where("target_type", "post")
    ])
    for link in links:
       models.link.remove(link["id"])



    links = models.link.pull([
      where("name", "last-retrieved")
    ])
    for link in links:
       models.link.remove(link["id"])


def clean_follows(task):
    links = models.link.pull([
        where("origin_type", "identity"),
        where("target_type", "source"),
        where("name", "follows")
    ])

    for link in links:
        identity = models.identity.get(link["origin_id"])
        if identity is None:
            models.link.remove(link["id"])



def workbench(task):
    reset_all_posts()