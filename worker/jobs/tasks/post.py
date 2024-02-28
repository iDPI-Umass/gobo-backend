import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def pull_posts_from_source(task):
    source = h.enforce("source", task)
    
    link = models.link.random([
        where("origin_type", "identity"),
        where("target_type", "source"),
        where("target_id", source["id"]),
        where("name", "follows")
    ])
    if link is None:
        return task.halt()
    
    identity = models.identity.get(link["origin_id"])
    if identity is None:
        models.link.remove(link["id"])
        queues.default.put_task(task)
        return

    queues.default.put_details(
        name = "flow - pull posts",
        priority = task.priority,
        details = {
            "identity": identity,
            "source": source
        }
    )


def pull_posts(task):
    client = h.enforce("client", task)
    source = h.enforce("source", task)
    last_retrieved = task.details.get("last_retrieved", None)
    is_shallow = task.details.get("is_shallow", False)
    graph = client.get_post_graph(
        source = source, 
        last_retrieved = last_retrieved, 
        is_shallow = is_shallow
    )
    # Special case for early returns.
    if graph == False:
        task.halt()
        return
    return {"graph": graph}


def map_posts(task):
    client = h.enforce("client", task)
    graph = h.enforce("graph", task)
    graph["sources"] = h.enforce("sources", task)
    post_data = client.map_posts(graph)
    is_list = graph.get("is_list", False)
    return {
        "post_data": post_data,
        "is_list": is_list
    }


# Careful here: There is order dependency on getting full and partial posts
# in the database and associated with IDs. That way we guarantee integrity
# of the graph we build out of the edges. Afterward, the followers can be
# full throttle without order considerations.
def upsert_posts(task):
    seen_posts = set()
    all_posts = []
    def collect_post(post):
        if post["id"] not in seen_posts:
            seen_posts.add(post["id"])
            all_posts.append(post)

    seen_edges = set()
    all_edges = []
    def collect_edge(edge):
        if edge["id"] not in seen_edges:
            seen_edges.add(edge["id"])
            all_edges.append(edge)


    post_data = h.enforce("post_data", task)
    is_list = h.enforce("is_list", task)
    full_posts = []
    references = {}      

    for _post in post_data["posts"]:
        post = models.post.upsert(_post)
        full_posts.append(post)
        references[post["platform_id"]] = post
        h.attach_post(post)
        collect_post(post)
    for _post in post_data["partials"]:
        post = models.post.upsert(_post)
        references[post["platform_id"]] = post
        h.attach_post(post)
        collect_post(post)
    for edge in post_data["edges"]:
        origin = references.get(edge["origin_reference"], None)
        target = references.get(edge["target_reference"], None)
        
        if origin is None:
            logging.warning("upsert posts: origin post is not available")
            continue
        if target is None:
            logging.warning(f"upsert posts: target post is not available")
            continue

        link = models.link.upsert({
            "origin_type": "post",
            "origin_id": origin["id"],
            "target_type": "post",
            "target_id": target["id"],
            "name": edge["name"],
            "secondary": f"{target['published']}::{target['id']}"
        })
        collect_edge(link)

    if is_list == True:
        source = h.enforce("source", task)
        for post in full_posts:
            queues.default.put_details(
                name = "add post to list followers", 
                priority = task.priority,
                details = {
                    "source": source,
                    "post": post
                }
            )
    else:
        for post in full_posts:
            queues.default.put_details(
                name = "add post to followers", 
                priority = task.priority,
                details = {"post": post}
            )

    return {
        "posts": all_posts,
        "edges": all_edges
    }



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


def add_post_to_list_followers(task):
    page = task.details.get("page") or 1
    per_page = task.details.get("per_page") or 1000
    source = h.enforce("source", task)
    post = h.enforce("post", task)

    followers = models.link.query({
        "page": page,
        "per_page": per_page,
        "where": [
            where("origin_type", "identity"),
            where("target_type", "source"),
            where("target_id", source["id"]),
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