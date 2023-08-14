import logging
import joy
import models
import queues
from clients import Reddit
from .helpers import set_identity_follow_fanout
from .helpers import set_pull_sources
from .helpers import set_read_sources
from .helpers import set_read_source
from .helpers import set_pull_posts

where = models.helpers.where


def dispatch(task):
    task.handler = "reddit"
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
    elif task.name == "create post":
        create_post(task)
    elif task.name == "workbench":
        workbench(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


identity_follow_fanout = set_identity_follow_fanout(
    where_statements = [ where("base_url", Reddit.BASE_URL) ],
    queue = queues.reddit
)

pull_sources = set_pull_sources(
    Client = Reddit,
    queue = queues.reddit
)

read_sources = set_read_sources(
    where_statements = [ where("base_url", Reddit.BASE_URL) ],
    queue = queues.reddit
)

read_source = set_read_source(
    Client = Reddit,
    queue = queues.reddit
)

pull_posts = set_pull_posts(
    queue = queues.reddit
)


# def workbench(task):
#     client = Reddit(task.details["identity"])
    
#     # id = "14f9q07" # Submission that's crossposted
#     # client.get_post(id)
    
#     # ids = ["t3_14f6btr"]
#     # client.pluck_posts(ids)

#     id = "10yt1ch" # Submission with poll
#     client.get_post(id)


def pull_sources_after_onboarding(task):
    sources = pull_sources(task)
    for source in sources:
        queues.reddit.put_details("read source", {
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
        where("base_url", Reddit.BASE_URL)
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


    queues.reddit.put_details("read sources", {})



def create_post(task):
    identity = task.details.get("identity", None)
    if identity is None:
        raise Exception("reddit: create_post requires identity")
    post = task.details.get("post", None)
    if post is None:
        raise Exception("reddit: create_post requires post")
    metadata = task.details.get("metadata", {})

    client = Reddit(identity)
    client.create_post(post, metadata)



def workbench(task):
    posts = models.post.pull([
        where("base_url", Reddit.BASE_URL)
    ])

    ids = []
    for post in posts:
        if not post["platform_id"].startswith("t3_"):
            ids.append(post["id"])

    links = models.link.pull([
        where("origin_type", "post"),
        where("origin_id", ids, "in")
    ])

    for link in links:
        models.link.remove(link["id"])

    for id in ids:
        models.post.remove(id)