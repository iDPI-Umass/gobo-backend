import logging
import models
import joy
from clients import Bluesky, Reddit, Mastodon
import queues

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator
supported_platforms = [
  "all",
  "bluesky",
  "mastodon",
  "reddit",
  "smalltown"
]



def is_valid_platform(platform):
  return platform in supported_platforms


def get_platform(identity):
    platform = identity.get("platform", None)
    if not is_valid_platform(platform):
        raise Exception(f"{platform} is an invalid platform")
    return platform


def enforce(name, task):
    value = task.details.get(name, None)
    if value is None:
        raise Exception(f"task requires field {name} to be specified")
    return value


def get_client(identity):
    platform = identity["platform"]

    if platform == "bluesky":
        client = Bluesky(identity)
    elif platform == "mastodon":
        base_url = identity["base_url"]
        mastodon_client = models.mastodon_client.find({"base_url": base_url})
        if mastodon_client == None:
            logging.warning(f"no mastodon client found for {base_url}")
            return
        client = Mastodon(mastodon_client, identity)
    elif platform == "reddit":
        client = Reddit(identity)
    else:
        raise Exception("unknown platform")
    
    return client

def reconcile_sources(identity, sources):
    desired_sources = []
    for source in sources:
        desired_sources.append(source["id"])
    
    results = models.link.pull([
        where("origin_type", "identity"),
        where("origin_id", identity["id"]),
        where("target_type", "source"),
        where("name", "follows")
    ])
   
    current_sources = []
    source_ids = [ result["target_id"] for result in results ]
    for source in models.source.pluck(source_ids):
        if source["base_url"] == identity["base_url"]:
            current_sources.append(source["id"])


    difference = list(set(desired_sources) - set(current_sources))
    for source_id in difference:
        logging.info(f"For identity {identity['id']}, adding source {source_id}")
        queues.default.put_details("follow", {
            "identity_id": identity["id"],
            "source_id": source_id
        })

    difference = list(set(current_sources) - set(desired_sources))
    for source_id in difference:
        logging.info(f"For identity {identity['id']}, removing source {source_id}")
        queues.default.put_details("unfollow", {
            "identity_id": identity["id"],
            "source_id": source_id
        })


def attach_post(post):
    models.link.upsert({
        "origin_type": "source",
        "origin_id": post["source_id"],
        "target_type": "post",
        "target_id": post["id"],
        "name": "has-post",
        "secondary": f"{post['published']}::{post['id']}"
    })