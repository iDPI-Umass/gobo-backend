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