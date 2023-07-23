import logging
import joy
import models
import queues
from clients import Twitter, Reddit, Mastodon
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
    elif task.name == "clear last retrieved":
        clear_last_retrieved(task)
    elif task.name == "clear all last retrieved":
        clear_all_last_retrieved(task)
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
    queue = queues.mastodon
)

read_source = set_read_source(
    Client = Mastodon,
    queue = queues.mastodon
)

pull_posts = set_pull_posts(
    queue = queues.mastodon
)

def clear_last_retrieved(task):
    url = task.details.get("url")
    if url is None:
        raise Exception("clear last retrieved: needs target url to find source")
    
    sources = models.source.pull([
      where("url", url)
    ])
    logging.info(sources)
    source = None
    if len(sources) > 0:
        source = sources[0]
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
    results = models.identity.pull([ 
        where("base_url", Twitter.BASE_URL, "neq"),
        where("base_url", Reddit.BASE_URL, "neq")
    ])
    identities = []
    for i in results:
        identities.append(i.id)
    

    links = models.link.pull([
        where("name", "last-retrieved"),
        where("origin_type", "source"),
        where("origin_id", identities, "in")
    ])

    for link in links:
        link["secondary"] = None
        models.link.upsert(link)


    queues.mastodon.put_details("read sources", {})