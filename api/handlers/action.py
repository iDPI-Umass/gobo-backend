import logging
from flask import request, g
import http_errors
import models
from platform_models import twitter, reddit, mastodon
from .helpers import parse_base_url


def action_onboard_identity_start_post():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    base_url = parse_base_url(request.json)

    if base_url == twitter.BASE_URL:
        redirect_url = twitter.get_redirect_url(person)
    elif base_url == reddit.BASE_URL:
        redirect_url = reddit.get_redirect_url(person)
    else:
        redirect_url = mastodon.get_redirect_url(person, base_url)

    return {"redirect_url": redirect_url}
  

def action_onboard_identity_callback_post():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    base_url = parse_base_url(request.json)
    
    registration = models.registration.find({
        "person_id": person["id"],
        "base_url": base_url
    })

    if registration == None:
        http_errors.unprocessable_content("this person has no pending registration with this provider")


    if base_url == twitter.BASE_URL:
        data = twitter.validate_callback(request.json)
        identity = twitter.confirm_identity(registration, data)
    elif base_url == reddit.BASE_URL:
        data = reddit.validate_callback(request.json)
        identity = reddit.confirm_identity(registration, data)
    else:
        data = mastodon.validate_callback(request.json, base_url)
        identity = mastodon.confirm_identity(registration, data)

    return identity