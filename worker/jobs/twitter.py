import logging
import joy
import models
import queues
from clients import Twitter
from .helpers import set_identity_follow_fanout
from .helpers import set_pull_sources
from .helpers import set_pull_posts

where = models.helpers.where


def dispatch(task):
    task.handler = "twitter"
    logging.info("dispatching: %s", task)

    if task.name == "identity follow fanout":
        identity_follow_fanout(task)
    elif task.name == "pull sources":
        pull_sources(task)
    elif task.name == "pull posts":
        pull_posts(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


identity_follow_fanout = set_identity_follow_fanout(
    where_statement = [ where("base_url", Twitter.BASE_URL) ],
    queue = queues.twitter
)

pull_sources = set_pull_sources(
    Client = Twitter,
    queue = queues.twitter
)

pull_posts = set_pull_posts(
    queue = queues.twitter
)