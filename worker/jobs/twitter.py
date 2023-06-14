import logging
from queues import twitter as queue
from clients import Twitter
import models


def dispatch(task):
    task.handler = "twitter"
    logging.info("dispatching: %s", task)

    if task.name == "start pull sources":
        start_pull_sources(task)
    elif task.name == "pull sources":
        pull_sources(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()

def start_pull_sources(task):
    page = task.details.get("page") or 1
    query = {
        "per_page": 500,
        "page": page,
        "where": [{
            "key": "base_url",
            "value": Twitter.BASE_URL,
            "operator": "eq"
        }]
    }

    identities = models.identity.query(query)
    for identity in identities:
        queue.put("pull sources", {"identity": identity})

    if len(identities) == 500:
        task.update({"page": page + 1})
        queue.put(task)

def pull_sources(task):
    identity = task.details.get("identity")
    if identity == None:
        logging.error("pull posts task requires an identity to run, bailing")
        return

    twitter = Twitter(identity)
    _sources = twitter.list_sources()
    sources = models.source.store_twitter_sources(_sources)

    for source in sources:
        models.link.safe_add({
            "origin_type": "person",
            "origin_id": identity["person_id"],
            "target_type": "source",
            "target_id": source["id"],
            "name": "follows",
            "secondary": None
        })


        queue.put("pull posts", {
            "twitter": twitter,
            "identity": identity,
            "source": source,
        })


def pull_posts(task):
    twitter = task.details.get("twitter")
    identity = task.details.get("identity")
    source = task.details.get("source")
    if twitter == None or identity == None or source == None:
        logging.error("pull posts task lacks needed inputs, bailing")
        return

    _posts = twitter.get_posts(source)
    posts = models.post.store_twitter_posts(source, _posts)
    for post in posts:
        models.link.safe_add({
            "origin_type": "person",
            "origin_id": identity["person_id"],
            "target_type": "post",
            "target_id": post["id"],
            "name": "full-feed",
            "secondary": None
        })