import logging
import os
import models
import queues
from clients import Bluesky
from .helpers import set_identity_follow_fanout
from .helpers import set_pull_sources
from .helpers import set_onboard_sources
from .helpers import set_read_sources
from .helpers import set_read_source
from .helpers import set_pull_posts

where = models.helpers.where


def dispatch(task):
    task.handler = "bluesky"
    logging.info("dispatching: %s", task)

    if task.name == "identity follow fanout":
        identity_follow_fanout(task)
    elif task.name == "pull sources":
        pull_sources(task)
    elif task.name == "onboard sources":
        onboard_sources(task)
    elif task.name == "read sources":
        read_sources(task)
    elif task.name == "read source":
        read_source(task)
    elif task.name == "pull posts":
        pull_posts(task)
    elif task.name == "clear last retrieved":
        clear_last_retrieved(task)
    elif task.name == "clear all last retrieved":
        clear_all_last_retrieved(task)
    elif task.name == "hard reset posts":
        hard_reset_posts(task)
    elif task.name == "create post":
        create_post(task)
    elif task.name == "add post edge":
        add_post_edge(task)
    elif task.name == "remove post edge":
        remove_post_edge(task)
    elif task.name == "workbench":
        workbench(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


identity_follow_fanout = set_identity_follow_fanout(
    where_statements = [
      where("base_url", Bluesky.BASE_URL)
    ],
    queue = queues.bluesky
)

pull_sources = set_pull_sources(
    Client = Bluesky,
    queue = queues.bluesky
)

onboard_sources = set_onboard_sources(
    Client = Bluesky,
    queue = queues.bluesky
)

read_sources = set_read_sources(
    where_statements = [ 
        where("base_url", Bluesky.BASE_URL)
    ],
    queue = queues.bluesky
)

read_source = set_read_source(
    Client = Bluesky,
    queue = queues.bluesky
)

pull_posts = set_pull_posts(
    queue = queues.bluesky
)


def clear_last_retrieved(task):
    url = task.details.get("url")
    if url is None:
        raise Exception("clear last retrieved: needs target url to find source")
    
    source = models.source.find({"url": url})
    if source is None:
        raise Exception("clear last retrireved: no matching source was found for this task")
    
    link = models.link.find({
        "origin_type": "source",
        "origin_id": source["id"],
        "target_type": "source",
        "target_id": source["id"],
        "name": "last-retrieved"
    })

    link["secondary"] = None
    models.link.upsert(link)
    queues.bluesky.put_details("read source", {"source": source})

    

def clear_all_last_retrieved(task):
    results = models.source.pull([ 
        where("base_url", Bluesky.BASE_URL)
    ])

    sources = []
    for result in results:
        sources.append(result["id"])
    

    links = models.link.pull([
        where("name", "last-retrieved"),
        where("origin_type", "source"),
        where("origin_id", sources, "in")
    ])

    for link in links:
        link["secondary"] = None
        logging.info(link)
        models.link.upsert(link)


    queues.bluesky.put_details("read sources", {})



def hard_reset_posts(task):
    posts = models.post.pull([
        where("base_url", Bluesky.BASE_URL)
    ])

    for post in posts:
        queues.database.put_details( "remove post", {
            "post": post
        })



def create_post(task):
    identity = task.details.get("identity", None)
    if identity is None:
        raise Exception("bluesky: create_post requires identity")
    post = task.details.get("post", None)
    if post is None:
        raise Exception("bluesky: create_post requires post")
    metadata = task.details.get("metadata", {})

    attachments = []
    for draft in post["attachments"]:
        filename = os.path.join(os.environ.get("UPLOAD_DIRECTORY"), draft["id"])
        if os.path.exists(filename):
            with open(filename, "rb") as f:
                _draft = dict(draft)
                _draft["data"] = f.read()
                attachments.append(_draft)

    post["attachments"] = attachments

    client = Bluesky(identity)
    client.create_post(post, metadata)
    logging.info("bluesky: create post complete")
    for draft in post["attachments"]:
        draft["published"] = True
        models.draft_image.update(draft["id"], draft)


def add_post_edge(task):
    identity = task.details.get("identity", None)
    if identity is None:
        raise Exception("bluesky: add_post_edge requires identity")
    post = task.details.get("post", None)
    if post is None:
        raise Exception("bluesky: add_post_edge requires post")
    name = task.details.get("name", None)
    if name is None:
        raise Exception("bluesky: add_post_edge requires name")
    edge = task.details.get("edge", None)
    if edge is None:
        raise Exception("blusky: add_post_edge requires edge")

    if name in ["like", "repost"]:
        client = Bluesky(identity)
        if name == "like":
            like_edge = client.like_post(post)
            edge["stash"] = like_edge
            models.post_edge.update(edge["id"], edge)
            logging.info(f"bluesky: like post complete on {post['id']}")
        elif name == "repost":
            repost_edge = client.repost_post(post)
            edge["stash"] = repost_edge
            models.post_edge.update(edge["id"], edge)
            logging.info(f"bluesky: repost post complete on {post['id']}")
    else:
        raise logging.warning(
            f"bluesky does not have post edge action defined for {name}"
        )

def remove_post_edge(task):
    identity = task.details.get("identity", None)
    if identity is None:
        raise Exception("bluesky: remove_post_edge requires identity")
    post = task.details.get("post", None)
    if post is None:
        raise Exception("bluesky: remove_post_edge requires post")
    name = task.details.get("name", None)
    if name is None:
        raise Exception("bluesky: remove_post_edge requires name")
    edge = task.details.get("edge", None)
    if edge is None:
        raise Exception("blusky: add_post_edge requires edge")

    if name in ["like", "repost"]:
        client = Bluesky(identity)
        if name == "like":
            client.undo_like_post(edge)
            logging.info(f"bluesky: undo like post complete on {post['id']}")
        elif name == "repost":
            client.undo_repost_post(edge)
            logging.info(f"bluesky: undo repost post complete on {post['id']}")
    else:
        raise logging.warning(
            f"bluesky does not have post edge action defined for {name}"
        )


def workbench(task):
    identity = models.identity.find({
        "profile_url": "https://bsky.app/profile/freeformflow.bsky.social"
    })

    source = models.source.find({
        "url": task.details.get("url")
    })
    
    client = Bluesky(identity)
    result = client.client.get_author_feed(source["username"], None)
    feed = result["feed"]
    results = []
    for post in feed:
        p = Bluesky.build_post(post)
        logging.info(str(p))
        logging.info("\n\n")