import logging
import models
from clients import Bluesky
import http_errors
import joy

# result = client.client.bsky.actor.get_profile({"actor": "freeformflow.bsky.social"})
# result = client.client.bsky.feed.get_author_feed({"actor": "noupside.bsky.social"})
# logging.info(result)

BASE_URL = Bluesky.BASE_URL

def get_redirect_url(person):
    state = joy.crypto.random({"encoding": "safe-base64"})

    _registration = {
        "person_id": person["id"],
        "base_url": BASE_URL,
        "state": state
    }

    registration = models.registration.find({
      "person_id": person["id"],
      "base_url": BASE_URL
    })

    if registration == None:
        models.registration.add(_registration)
    else:
        models.registration.update(registration["id"], _registration)

    # No redirect URL. We need the Bluesky account owner through a more manual process.
    return {"state": state}

def validate_callback(data):
    output = {
      "bluesky_login": data.get("bluesky_login", None),
      "bluesky_secret": data.get("bluesky_secret", None),
      "state": data.get("state", None)
    }

    if output["bluesky_login"] == None:
        raise http_errors.bad_request("field bluesky_login is required")
    if output["bluesky_secret"] == None:
        raise http_errors.bad_request("field bluesky_secret is required")
    if output["state"] == None:
        raise http_errors.bad_request("field state is required")

    return output

def confirm_identity(registration, data):
    # Check the nonce
    if registration["state"] != data["state"]:
        logging.warning("bluesky onboarding nonce does not match") 
        raise http_errors.unprocessable_content("unable to process provider credentials")


    # Establish a Bluesky session associated with this identity.
    try:
        session = Bluesky.login({
            "oauth_token": data["bluesky_login"],
            "oauth_token_secret": data["bluesky_secret"]
        })
    except Exception as e:
        logging.warning(e)
        raise http_errors.unprocessable_content("unable to access profile from platform")

    # Pull together data to build a session record. A session record must be
    # available to support proper function of the client. Sessions are part of
    # the client's internal interface to maintain authorization.
    _session = Bluesky.map_session(registration, session)
    session = models.bluesky_session.upsert(_session)


    # Now it's safe to use the main class instantiation.
    client = Bluesky({
        "person_id": registration["person_id"],
        "platform_id": session["did"],
        "base_url": BASE_URL,
        "oauth_token": data["bluesky_login"],
        "oauth_token_secret": data["bluesky_secret"],
    })
    client.login()
    profile = client.get_profile()


    # Pull together data to build an identity record.
    profile_url = f"{BASE_URL}/profile/{profile['handle']}"  
    _identity = {
        "person_id": registration["person_id"],
        "platform_id": profile["did"],
        "base_url": BASE_URL,
        "profile_url": profile_url,
        "profile_image": profile.get("avatar", None),
        "username": profile["handle"],
        "name": profile.get("displayName", None),
        "oauth_token": data["bluesky_login"],
        "oauth_token_secret": data["bluesky_secret"]
    }


    # Store and finalize
    identity = models.identity.upsert(_identity)

    models.link.upsert({
      "origin_type": "person",
      "origin_id": identity["person_id"],
      "target_type": "identity",
      "target_id": identity["id"],
      "name": "has-identity",
      "secondary": None
    })

    models.task.add({
        "queue": "bluesky",
        "name": "onboard sources",
        "details": {
            "identity": identity
        }
    })
    
    models.registration.remove(registration["id"])

    return identity

