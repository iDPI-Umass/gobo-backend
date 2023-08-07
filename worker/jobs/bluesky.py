import logging
import re
import json
import joy
import models
import queues
from clients import Bluesky
from .helpers import set_identity_follow_fanout
from .helpers import set_pull_sources
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


def pull_sources_after_onboarding(task):
    sources = pull_sources(task)
    for source in sources:
        queues.bluesky.put_details("read source", {
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



# def workbench(task):
#     sources = models.source.pull([
#         where("base_url", Bluesky.BASE_URL)
#     ])

#     for source in sources:
#         links = models.link.pull([
#             where("origin_type", "source"),
#             where("origin_id", source["id"]),
#             where("target_type", "post"),
#             where("name", "has-post")
#         ])

#         for link in links:
#             queues.database.put_details( "remove post", {
#                 "post": {
#                     "id": link["target_id"]
#                 }
#             })


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