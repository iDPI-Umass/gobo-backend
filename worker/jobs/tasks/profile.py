import logging
import mastodon
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


# TODO: See Bluesky cycle refresh tokens. There's a mapping problem here.
#   We need to detect revocation by watching for specific response codes,
#   then map that to the proper identity removal in Gobo. But handling this
#   kind of exceptional, branching flow is messy without something like
#   talos to separate out the paths.


def get_profile(task):
    identity = h.enforce("identity", task)
    client = h.enforce("client", task)

    try:
        profile = client.get_profile()
    except mastodon.errors.MastodonUnauthorizedError as e:
        junk, status, status_description, message = e.args
        if status == 401 and message == "The access token is invalid":
            logging.warning("detected revoked Mastodon token, removing identity")
            queues.default.put_details(
                priority = 1,
                name = "remove identity",
                details = {"identity": identity}
            )
            task.halt()
            return

    return {"profile": profile}


def map_profile(task):
    client = h.enforce("client", task)
    identity = h.enforce("identity", task)
    profile = h.enforce("profile", task)
    identity = client.map_profile({
        "profile": profile, 
        "identity": identity
    })
    return {"identity": identity}


def upsert_profile(task):
    identity = h.enforce("identity", task)
    models.identity.upsert(identity)