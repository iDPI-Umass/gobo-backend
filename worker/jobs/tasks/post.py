import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def pull_posts_fanout(task):
    platform = h.get_platform(task.details)
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]

    identities = QueryIterator(
        model = models.identity,
        wheres = wheres
    )
    for identity in identities:
        links = QueryIterator(
            model = models.link,
            wheres = [
                where("origin_type", "identity"),
                where("origin_id", identity["id"]),
                where("target_type", "source"),
                where("name", "follows")
            ]
        )
        for link in links:
            source = models.source.get(link["target_id"])
            if source is None:
                logging.warning(f"identity {identity['id']} follows source {link['target_id']}, but that source does not exist")
                continue
            else:
                queues.default.put_details("flow - pull posts", {
                    "identity": identity,
                    "source": source
                })


def get_last_retrieved(task):
    source = h.enforce("source", task)
    
    should_fetch = False
    loop = models.source.get_last_retrieved(source["id"])
    last_retrieved = loop.get("secondary", None)
    if last_retrieved is None:
        should_fetch = True
        new_last_retrieved = joy.time.now()
    else:
        date = joy.time.convert("iso", "date", last_retrieved)
        if joy.time.latency(date).seconds > 600:
            should_fetch = True
            new_last_retrieved = joy.time.now()

    if should_fetch == False:
        return task.halt()

    return {
        "last_retrieved_loop": loop,
        "last_retrieved": last_retrieved,
        "new_last_retrieved": new_last_retrieved
    }


def set_last_retrieved(task):
    link = h.enforce("last_retrieved_loop", task)
    value = h.enforce("new_last_retrieved", task)
    link["secondary"] = value
    models.link.update(link["id"], link)


def pull_posts(task):
    identity = h.enforce("identity", task)
    source = h.enforce("source", task)
    last_retrieved = task.details.get("last_retrieved", None)
    is_shallow = task.details.get("is_shallow", False)
    client = h.get_client(identity)
    client.login()
    graph = client.get_post_graph(
        source = source, 
        last_retrieved = last_retrieved, 
        is_shallow = is_shallow
    )
    return {"graph": graph}


def map_posts(task):
    identity = h.enforce("identity", task)
    graph = h.enforce("graph", task)
    sources = h.enforce("sources", task)
    graph["sources"] = sources
    client = h.get_client(identity)
    post_data = client.map_posts(graph)
    return {"post_data": post_data}


# Careful here: There is order dependency on getting full and partial posts
# in the database and associated with IDs. That way we guarantee integrity
# of the graph we build out of the edges. Afterward, the followers can be
# full throttle without order considerations.
def upsert_posts(task):
    post_data = h.enforce("post_data", task)
    full_posts = []
    references = {}      

    for _post in post_data["posts"]:
        post = models.post.upsert(_post)
        full_posts.append(post)
        references[post["platform_id"]] = post
        h.attach_post(post)     
    for _post in post_data["partials"]:
        post = models.post.safe_add(_post)
        references[post["platform_id"]] = post
        h.attach_post(post)
    for edge in post_data["edges"]:
        origin = references.get(edge["origin_reference"], None)
        target = references.get(edge["target_reference"], None)
        
        if origin is None:
            logging.warning("upsert posts: origin post is not available")
            continue
        if target is None:
            logging.warning(f"upsert posts: target post is not available")
            continue

        models.link.upsert({
            "origin_type": "post",
            "origin_id": origin["id"],
            "target_type": "post",
            "target_id": target["id"],
            "name": edge["name"],
            "secondary": f"{target['published']}::{target['id']}"
        })

    for post in full_posts:
        queues.default.put_details("add post to followers", {"post": post})\



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
    h.remove_post(post)