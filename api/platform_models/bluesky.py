import logging
import models
from clients.bluesky import Bluesky, SessionFrame
from clients.gobo_bluesky import GOBOBluesky
import http_errors
import joy

BASE_URL = Bluesky.BASE_URL


def get_redirect_url(person):
    state = joy.crypto.random({"encoding": "safe-base64"})

    _registration = {
        "person_id": person["id"],
        "platform": "bluesky",
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
        client = GOBOBluesky()
        bundle = client.login(
            login = data["bluesky_login"],
            password = data["bluesky_secret"]
        )
    except Exception as e:
        logging.warning(e)
        raise http_errors.unprocessable_content("unable to access profile from platform")

    # Pull together a preliminary identity record. We'll need a valid ID
    # for the other components.
    _identity = {
        "person_id": registration["person_id"],
        "platform": "bluesky",
        "platform_id": bundle["did"],
        "base_url": BASE_URL,
        "oauth_token": data["bluesky_login"],
        "oauth_token_secret": data["bluesky_secret"],
        "stale": False
    }
    identity = models.identity.upsert(_identity)


    # Now we can pull together a valid session record. Such a record must be
    # available to support the proper function of the Bluesky client module.
    # It's part of the internal authorization inteface.
    _session = SessionFrame.map(identity, bundle)
    models.bluesky_session.upsert(_session)

    # Now it's safe to use the main class instantiation.
    client = Bluesky(identity)
    client.login()
    profile = client.get_profile()


    # Fill out the identity with properties from the platform.
    handle = profile["handle"]
    if handle == "handle.invalid":
        handle = profile["did"]
    
    identity["profile_url"] = f"{BASE_URL}/profile/{handle}"
    identity["profile_image"] = profile.get("avatar", None)
    identity["username"] = handle
    identity["name"] = profile.get("displayName", None)

    # Store and finalize
    identity = models.identity.upsert(identity)

    models.link.upsert({
      "origin_type": "person",
      "origin_id": identity["person_id"],
      "target_type": "identity",
      "target_id": identity["id"],
      "name": "has-identity",
      "secondary": None
    })

    models.task.add({
        "queue": "default",
        "name": "flow - update identity",
        "priority": 1,
        "details": {
            "identity": identity,
            "is_onboarding": True
        }
    })
    
    models.registration.remove(registration["id"])

    return identity