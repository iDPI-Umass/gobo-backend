import logging
import os
import models
import queues
from clients import Bluesky, Reddit, Mastodon
from .helpers import set_identity_follow_fanout
from .helpers import set_pull_sources
from .helpers import set_read_sources
from .helpers import set_read_source
from .helpers import set_pull_posts

where = models.helpers.where


def dispatch(task):
    task.handler = "mastodon"
    logging.info("dispatching: %s", task)

    if task.name == "identity follow fanout":
        identity_follow_fanout(task)
    elif task.name == "pull sources":
        pull_sources(task)
    elif task.name == "read sources":
        read_sources(task)
    elif task.name == "read source":
        read_source(task)
    elif task.name == "pull posts":
        pull_posts(task)
    elif task.name == "pull sources after onboarding":
        pull_sources_after_onboarding(task)
    elif task.name == "clear last retrieved":
        clear_last_retrieved(task)
    elif task.name == "clear all last retrieved":
        clear_all_last_retrieved(task)
    elif task.name == "hard reset posts":
        hard_reset_posts(task)
    elif task.name == "create post":
        create_post(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


identity_follow_fanout = set_identity_follow_fanout(
    where_statements = [ 
        where("base_url", Bluesky.BASE_URL, "neq"),
        where("base_url", Reddit.BASE_URL, "neq")
    ],
    queue = queues.mastodon
)

pull_sources = set_pull_sources(
    Client = Mastodon,
    queue = queues.mastodon
)

read_sources = set_read_sources(
    where_statements = [ 
        where("base_url", Bluesky.BASE_URL, "neq"),
        where("base_url", Reddit.BASE_URL, "neq")
    ],
    queue = queues.mastodon
)

read_source = set_read_source(
    Client = Mastodon,
    queue = queues.mastodon
)

pull_posts = set_pull_posts(
    queue = queues.mastodon
)


def pull_sources_after_onboarding(task):
    sources = pull_sources(task)
    for source in sources:
        queues.mastodon.put_details("read source", {
            "source": source
        })


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
    queues.mastodon.put_details("read source", {"source": source})

    

def clear_all_last_retrieved(task):
    results = models.source.pull([ 
        where("base_url", Bluesky.BASE_URL, "neq"),
        where("base_url", Reddit.BASE_URL, "neq")
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
        models.link.upsert(link)


    queues.mastodon.put_details("read sources", {})


def hard_reset_posts(task):
    posts = models.post.pull([
        where("base_url", Bluesky.BASE_URL, "neq"),
        where("base_url", Reddit.BASE_URL, "neq")
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


    base_url = identity["base_url"]
    mastodon_client = models.mastodon_client.find({"base_url": base_url})
    if mastodon_client == None:
        logging.warning(f"no mastodon client found for {base_url}")
        return
    
    client = Mastodon(mastodon_client, identity)
    client.create_post(post, metadata)
    for draft in post["attachments"]:
        draft["published"] = True
        models.draft_image.update(draft["id"], draft)