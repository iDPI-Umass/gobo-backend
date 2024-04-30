import logging
import json
import models
from clients.linkedin import Linkedin, SessionFrame
import http_errors
import joy

BASE_URL = Linkedin.BASE_URL


def get_redirect_url(person):
    scope = "profile openid w_member_social"
    state = joy.crypto.random({"encoding": "base16"})
    context = {
        "scope": scope,
        "state": state
    }
    url = Linkedin.make_login_url(context)

    _registration = {
        "person_id": person["id"],
        "platform": "linkedin",
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

    return {"redirect_url": url}

def validate_callback(data):
    output = {
      "state": data.get("state"),
      "code": data.get("code")
    }

    if output["state"] is None:
        raise http_errors.bad_request("field state is required")
    if output["code"] is None:
        raise http_errors.bad_request("field code is required")
    return output

def confirm_identity(registration, data):
    if registration.get("state") == None:
        raise http_errors.unprocessable_content("invalid registration, retry step 1 of identity onboarding")

    if registration.get("state") != data["state"]:
        raise http_errors.unprocessable_content("state doesn't match, retry step 1 of identity onboarding")

    # Convert the code into a durable OAuth token
    try:
        bundle = Linkedin.exchange_code(data["code"])
    except Exception as e:
        logging.warning(e)
        raise http_errors.unprocessable_content("unable to process provider credentials")


    # Pull together an identity record to support upserting a session.
    _identity = {
        "person_id": registration["person_id"],
        "platform": "linkedin",
        "platform_id": bundle["user"]["sub"],
        "base_url": BASE_URL,
        "profile_image": bundle["user"]["picture"],
        "username": bundle["user"]["name"],
        "oauth_token": bundle["tokens"]["access_token"],
        "stale": False
    }
    identity = models.identity.upsert(_identity)


    # A session is needed to support the proper function of the LinkedIn
    # client module's authorization inteface.
    _session = SessionFrame.from_bundle(identity, bundle)
    models.linkedin_session.upsert(_session)

    # Now it's safe to use the Linkedin class in the workers. We don't need
    # any more data, but we should confirm session access.
    client = Linkedin(identity)
    client.login()
   
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