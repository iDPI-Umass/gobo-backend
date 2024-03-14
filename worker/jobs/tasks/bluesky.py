import logging
import models
import joy
import queues
from clients import parse_mentions, parse_links
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def find_identity(session):
    return models.identity.find({
        "person_id": session["person_id"],
        "base_url": session["base_url"],
        "platform_id": session["did"]
    })


def test_facet_parsing(task):
    a = "@chandrn.bsky.app this post was created in GOBO."
    b = "Hey, @chandrn.bsky.app, this post was created in GOBO."
    c = "chandrn.bsky.app this post was created in GOBO."
    d = "this mention is at the end of the sentence @chand.bsky.app"

    logging.info(f"a {parse_mentions(a)}")
    logging.info(f"b {parse_mentions(b)}")
    logging.info(f"c {parse_mentions(c)}")
    logging.info(f"d {parse_mentions(d)}")

    a = "This is an example link: https://community.publicinfrastructure.org"
    b = "This example: community.publicinfrastructure.org is in the middle and has not protocol"
    c = "https://community.publicinfrastructure.org that example was at start at post"
    d = "community.publicinfrastructure.org that example was at start at post"

    logging.info(f"a {parse_links(a)}")
    logging.info(f"b {parse_links(b)}")
    logging.info(f"c {parse_links(c)}")
    logging.info(f"d {parse_links(d)}")