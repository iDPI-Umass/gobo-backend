import logging
import joy
import models
from clients import Reddit
import http_errors

BASE_URL = Reddit.BASE_URL


def get_redirect_url(person):
    client = Reddit()
    state = joy.crypto.random({"encoding": "safe-base64"})
    url = client.get_redirect_url(state)

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

    return  {"redirect_url": url}


def validate_callback(data):
    output = {
      "state": data.get("state", None),
      "code": data.get("code", None)
    }

    if output["state"] == None:
        raise http_errors.bad_request("field state is required")
    if output["code"] == None:
        raise http_errors.bad_request("field code is required")
    return output


def confirm_identity(registration, data):
    if registration.get("state") == None:
        raise http_errors.unprocessable_content("invalid registration, retry step 1 of identity onboarding")

    if registration.get("state") != data["state"]:
        raise http_errors.unprocessable_content("state doesn't match, retry step 1 of identity onboarding")

    # Convert the code into a durable OAuth token
    try:
        client = Reddit()
        oauth_token = client.convert_code(data["code"])
    except Exception as e:
        logging.warning(e)
        raise http_errors.unprocessable_content("unable to process provider credentials")


    # Fetch profile data to associate with this identity.
    try:
        client = Reddit({"oauth_token": oauth_token})
        profile = client.get_profile()
    except Exception as e:
        logging.warning(e)
        raise http_errors.unprocessable_content("unable to access profile from platform")
  

    # Pull together data to build an identity record.  
    profile_url = f"{BASE_URL}/user/{profile.name}"
    _identity = {
        "person_id": registration["person_id"],
        "platform_id": str(profile.id),
        "base_url": BASE_URL,
        "profile_url": profile_url,
        "profile_image": profile.icon_img,
        "username": profile.name,
        "name": profile.name,
        "oauth_token": oauth_token,
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
        "queue": "reddit",
        "name": "onboard sources",
        "details": {
            "identity": identity
        }
    })
    
    models.registration.remove(registration["id"])

    return identity
