import logging
import models
from clients import Twitter
import http_errors

BASE_URL = "https://twitter.com"

def get_redirect_url(person):
    user_handler = Twitter.get_user_handler(None)
    url = user_handler.get_authorization_url()
    token = user_handler.request_token["oauth_token"]
    secret = user_handler.request_token["oauth_token_secret"]

    _registration = {
        "person_id": person["id"],
        "base_url": BASE_URL,
        "oauth_token": token,
        "oauth_token_secret": secret
    }

    registration = models.registration.find({
      "person_id": person["id"],
      "base_url": BASE_URL
    })

    if registration == None:
        models.registration.add(_registration)
    else:
        models.registration.update(registration["id"], _registration)

    return url

def validate_callback(data):
    output = {
      "oauth_token": data.get("oauth_token"),
      "oauth_verifier": data.get("oauth_verifier")
    }

    if output["oauth_token"] == None:
        raise http_errors.bad_request("field oauth_token is required")
    if output["oauth_verifier"] == None:
        raise http_errors.bad_request("field oauth_verifier is required")
    return output

def confirm_identity(registration, data):
    # Get person-specific credentials
    try:
        registration["oauth_token"] = data["oauth_token"]
        user_handler = Twitter.get_user_handler(registration)
        result = user_handler.get_access_token(data["oauth_verifier"])
        oauth_token, oauth_token_secret = result
    except Exception as e:
        logging.warning(e)
        raise http_errors.unprocessable_content("unable to process provider credentials")


    # Fetch profile data to associate with this identity.
    try:
        client = Twitter({
            "oauth_token": oauth_token,
            "oauth_token_secret": oauth_token_secret
        })
        profile = client.get_profile()
    except Exception as e:
        logging.warning(e)
        raise http_errors.unprocessable_content("unable to access profile from platform")
  

    # Pull together data to build an identity record.
    profile_url = f"{BASE_URL}/{profile.username}"
    _identity = {
        "person_id": registration["person_id"],
        "base_url": BASE_URL,
        "profile_url": profile_url,
        "profile_image": profile.profile_image_url,
        "username": profile.username,
        "name": profile.name,
        "oauth_token": oauth_token,
        "oauth_token_secret": oauth_token_secret
    }

    # Store and finalize
    identity = models.identity.find({"profile_url": profile_url})
    if identity == None:
        identity = models.identity.add(_identity)
    else:
        identity = models.identity.update(identity["id"], _identity)

    models.link.safe_add({
      "origin_type": "person",
      "origin_id": identity["person_id"],
      "target_type": "identity",
      "target_id": identity["id"],
      "name": "has-identity"
    })
    
    models.registration.remove(registration["id"])

    return identity

