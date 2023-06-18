import logging
import joy
import models
from queues import twitter as queue
from clients import Twitter
from .helpers import reconcile_sources, where


def dispatch(task):
    task.handler = "twitter"
    logging.info("dispatching: %s", task)

    if task.name == "start pull sources":
        start_pull_sources(task)
    elif task.name == "pull sources":
        pull_sources(task)
    elif task.name == "add post to followers":
        add_post_to_followers(task)
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
        queues.twitter.put_details(task)


def pull_sources(task):
    identity = task.details.get("identity")
    if identity == None:
        raise Exception("pull posts task requires an identity to run, bailing")

    twitter = Twitter(identity)
    raw_sources = twitter.list_sources()
    _sources = twitter.map_sources(raw_sources)

    sources = []
    for _source in _sources:
        source = models.source.safe_add(_source)
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

    last_retrieved = joy.time.now()
    data = twitter.list_posts(source)
    _posts = twitter.map_posts(source, data)

    source["last_retrieved"] = last_retrieved
    models.source.update(source["id"], source)

    posts = []
    for _post in _posts:
        post = models.post.safe_add(_post)
        posts.append(post)
        models.link.safe_add({
            "origin_type": "source",
            "origin_id": source["id"],
            "target_type": "post",
            "target_id": post["id"],
            "name": "has-post",
            "secondary": None
        })

    for post in posts:
        queues.twitter.put_details("add post to followers", {
            "page": 1,
            "per_page": 500,
            "post": post
        })


def add_post_to_followers(task):
    page = task.details.get("page") or 1
    per_page = task.details.get("per_page") or 500
    post = task.details.get("post")
    if post == None:
        raise Exception("add posts to followers must have post defined, bailing")

    followers = models.link.query({
        page: page,
        per_page: per_page,
        where: [
            where("origin_type", "person"),
            where("target_type", "source"),
            where("target_id", post["source_id"]),
            where("name", "follows")
        ]
    })

    if len(followers) == per_page:
        task.update({"page": page + 1})
        queues.twitter.put_task(task)

    for follower in followers:
        models.link.safe_add({
            "origin_type": "person",
            "origin_id": follower["origin_id"],
            "target_type": "post",
            "target_id": post["id"],
            "name": "full-feed",
            "secondary": None
        })