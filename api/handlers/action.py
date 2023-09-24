import logging
from flask import request, g
import http_errors
import models
from platform_models import bluesky, reddit, mastodon
from . import helpers as h


def action_onboard_identity_start_post():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    platform = request.json["platform"]

    if platform == "bluesky":
        response = bluesky.get_redirect_url(person)
    elif platform == "mastodon":
        base_url = h.parse_base_url(request.json["base_url"])
        response = mastodon.get_redirect_url(person, base_url)
    elif platform == "reddit":
        response = reddit.get_redirect_url(person)

    return response
  

def action_onboard_identity_callback_post():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    platform = request.json["platform"]
    base_url = h.parse_base_url(request.json)
    
    registration = models.registration.find({
        "person_id": person["id"],
        "base_url": base_url
    })

    if registration is None:
        raise http_errors.unprocessable_content(
            "this person has no pending registration with this provider"
        )


    if platform == "bluesky":
        data = bluesky.validate_callback(request.json)
        identity = bluesky.confirm_identity(registration, data)
    elif platform == "mastodon":
        data = mastodon.validate_callback(request.json, base_url)
        identity = mastodon.confirm_identity(registration, data)
    elif platform == "reddit":
        data = reddit.validate_callback(request.json)
        identity = reddit.confirm_identity(registration, data)

    return identity


def action_pull_identity_sources_post():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    
    identity = models.identity.find({
        "person_id": person["id"],
        "profile_url": request.json["profile_url"]
    })

    if identity == None:
        raise http_errors.unprocessable_content(
            "this person has no identity with this provider"
        )

    platform = identity["platform"]
    task = models.task.add({
      "queue": platform,
      "name": "pull sources after onboarding",
      "details": {
        "identity": identity
      }
    })

    return task