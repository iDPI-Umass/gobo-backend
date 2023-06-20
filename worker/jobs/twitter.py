import logging
import joy
import models
import queues
from clients import Twitter
from .helpers import reconcile_sources

where = models.helpers.where


def dispatch(task):
    task.handler = "twitter"
    logging.info("dispatching: %s", task)

    if task.name == "start pull sources":
        start_pull_sources(task)
    elif task.name == "pull sources":
        pull_sources(task)
    elif task.name == "pull posts":
        pull_posts(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()


def start_pull_sources(task):
    page = task.details.get("page") or 1
    query = {
        "per_page": 500,
        "page": page,
        "where": [
            where("base_url", Twitter.BASE_URL)    
        ]
    }

    identities = models.identity.query(query)
    for identity in identities:
        queues.twitter.put_details("pull sources", {"identity": identity})

    if len(identities) == 500:
        task.update({"page": page + 1})
        queues.twitter.put_task(task)


def pull_sources(task):
    identity = task.details.get("identity")
    if identity == None:
        raise Exception("pull posts task requires an identity to run, bailing")

    twitter = Twitter(identity)
    data = twitter.list_sources()
    _sources = twitter.map_sources(data)

    sources = []
    for _source in _sources:
        source = models.source.upsert(_source)
        sources.append(source)
   
    reconcile_sources(identity["person_id"], sources)

    for source in sources:
        queues.twitter.put_details("pull posts", {
            "twitter": twitter,
            "source": source,
        })


def pull_posts(task):
    twitter = task.details.get("twitter")
    source = task.details.get("source")
    if twitter == None or source == None:
        raise Exception("pull posts task lacks needed inputs, bailing")

    link = models.source.get_last_retrieved(source["id"])
    source["last_retrieved"] = link.get("secondary")

    last_retrieved = joy.time.now()
    data = twitter.list_posts(source)
    link["secondary"] = last_retrieved
    models.link.update(link["id"], link)

    sources = []
    _sources = twitter.map_sources(data)
    for _source in _sources:
        source = models.source.upsert(_source)
        sources.append(source)
    data["authors"] = sources

    _posts = twitter.map_posts(source, data)
    posts = []
    for _post in _posts:
        post = models.post.upsert(_post)
        posts.append(post)
        models.link.upsert({
            "origin_type": "source",
            "origin_id": source["id"],
            "target_type": "post",
            "target_id": post["id"],
            "name": "has-post",
            "secondary": post["published"]
        })

    for post in posts:
        queues.database.put_details("add post to followers", {
            "page": 1,
            "per_page": 500,
            "post": post
        })


