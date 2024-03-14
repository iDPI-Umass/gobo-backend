import logging
import mastodon
import prawcore
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


# NOTE: In the except handles here, we watch for indications that the given
# identity has been revoked by the owner or their platform provider. In that
# case, we want to remove the given identity and avoid issuing requests that
# result in error-class responses that look suspicious to providers.
#
# For Bluesky, we handle this in the refresh token cycle. There's a TODO to
# rein in the code there a little, but this pattern works for OAuth-based
# integrations where there's a clear point where we've failed to send an
# authorized request to the provider platform.
#
# For the future, we should consider taking a less drastic action than
# removing the identity outright. The errors we look for are reasonably
# specific, so there's acceptably contained risk for now. But ideally,
# I'd like to "deactivate" an identity. But that brings up HX questions
# that are probably best addressed by just re-linking the identity for now.


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
        raise e
        
    except prawcore.exceptions.ResponseException as e:
        if e.response.status_code == 400:
            logging.warning("detected revoked Reddit token, removing identity")
            queues.default.put_details(
                priority = 1,
                name = "remove identity",
                details = {"identity": identity}
            )
            task.halt()
            return
        raise e


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