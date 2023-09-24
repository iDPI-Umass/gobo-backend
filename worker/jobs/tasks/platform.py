import logging
import models
import joy
import queues
from clients import Bluesky, Reddit
import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator

def resolve_platform(url):
    if url == Bluesky.BASE_URL:
        return "bluesky"
    elif url == Reddit.BASE_URL:
        return "reddit"
    else:
        return "mastodon"

def boostrap_platform_labels(task):
    logging.info("processing identities")
    identities = QueryIterator(
        model = models.identity,
    )
    for identity in identities:
        if identity.get("platform", None) is None:
            identity["platform"] = resolve_platform(identity["base_url"])
            models.identity.update(identity["id"], identity)

    logging.info("processing posts")
    posts = QueryIterator(
        model = models.post,
    )
    for post in posts:
        if post.get("platform", None) is None:
            post["platform"] = resolve_platform(post["base_url"])
            models.post.update(post["id"], post)

    logging.info("processing registrations")
    registrations = QueryIterator(
        model = models.registration,
    )
    for registration in registrations:
        if registration.get("platform", None) is None:
            registration["platform"] = resolve_platform(registration["base_url"])
            models.registration.update(registration["id"], registration)

    logging.info("processing sources")
    sources = QueryIterator(
        model = models.source,
    )
    for source in sources:
        if source.get("platform", None) is None:
            source["platform"] = resolve_platform(source["base_url"])
            models.source.update(source["id"], source)