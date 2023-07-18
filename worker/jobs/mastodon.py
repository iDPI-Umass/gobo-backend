import logging
import joy
import models
import queues
from clients import Twitter, Reddit, Mastodon
from .helpers import set_identity_follow_fanout
from .helpers import set_pull_sources
from .helpers import set_read_sources
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
    elif task.name == "pull posts":
        pull_posts(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


identity_follow_fanout = set_identity_follow_fanout(
    where_statements = [ 
        where("base_url", Twitter.BASE_URL, "neq"),
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
        where("base_url", Twitter.BASE_URL, "neq"),
        where("base_url", Reddit.BASE_URL, "neq")
    ],
    Client = Mastodon,
    queue = queues.mastodon
)

pull_posts = set_pull_posts(
    queue = queues.mastodon
)