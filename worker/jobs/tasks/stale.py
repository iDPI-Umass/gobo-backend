import logging
from functools import wraps
import mastodon
import prawcore
import queues
from clients import HTTPError, Bluesky
from . import helpers as h

# Gobo relies on the consent of our members to bridge functionality across
# provider platforms. The identities denote that consent, and at any time,
# that consent may be revoked, rendering this identity inert.
# 
# This decorator creates a resuable double-duty check that we can seamlessly
# compose this behavior to arbitrary tasks:
# 1. Prior to attempting a request, we must confirm that the identity is
#    still valid.
# 2. If we run into an response where we can infer consent has been revoked
#    independent of a Gobo flow, either explicitly or implicitly through expiration.
# 
# In both cases, we want to isolate the identity, avoid issuing requests that
# result in error-class responses (that look suspicious to providers), and
# visually represent the identity as invalid.

def handle_stale(f):
    @wraps(f)
    def decorated(task):
        identity = h.enforce("identity", task)
        platform = identity["platform"]

        # This handles (1) from above.
        reference = h.get_identity(identity["id"])
        if reference is None:
            logging.warning(f"unable to retrieve identity {identity['id']} from database")    
            task.halt()
            return
        if reference["stale"] == True:
            logging.warning("aborting task with stale identity")
            task.halt()
            return
        
        # This handles (2) from above.
        try:
            return f(task)    
        except mastodon.errors.MastodonUnauthorizedError as e:
            junk, status, status_description, message = e.args
            if status == 401:
                logging.warning("detected revoked Mastodon token, stale identity")
                queues.default.put_details(
                    priority = 1,
                    name = "stale identity",
                    details = {"identity": identity}
                )
                task.halt()
                return
            raise e
        
        except prawcore.exceptions.ResponseException as e:
            # TODO: Something is wrong here. I believe we correctly configured
            # praw, but this is too big to be a bug in praw or prawcore.
            # When prawcore tries to use an invalid refresh OAuth token to
            # gain access to the API, it gets back a 400 response to some
            # request it makes, which it then raises as a generic reponse
            # exception, instead of one specifcially related to an invalid token.
            # It's okay for now because of our limited use of the Reddit API,
            # but this is bad.
            # See: https://github.com/praw-dev/prawcore/blob/main/prawcore/auth.py
        
            logging.warning("detected revoked Reddit token, stale identity")
            queues.default.put_details(
                priority = 1,
                name = "stale identity",
                details = {"identity": identity}
            )
            task.halt()
            return
        
        except HTTPError as e:
            if platform == "linkedin" and e.status == 401:
                logging.warning("detected revoked LinkedIn token, stale identity")
                queues.default.put_details(
                    priority = 1,
                    name = "stale identity",
                    details = {"identity": identity}
                )
                task.halt()
                return
            if platform == "bluesky" and e.status == 400:
                error = e.body.get("error")
                if error == "ExpiredToken":
                    try:
                        # For some reason, Bluesky expiry has been especially
                        # challenging to detect. This should doublecheck that
                        # we cannot restore our credentials if we get here.
                        
                        # TODO: We need a more graceful way to handle this.

                        client = Bluesky(identity)
                        client.login()
                        client.get_profile()
                    except:
                        logging.warning("detected revoked Bluesky token, stale identity")
                        queues.default.put_details(
                            priority = 1,
                            name = "stale identity",
                            details = {"identity": identity}
                        )
                        task.halt()
                        return
            raise e
        
    return decorated