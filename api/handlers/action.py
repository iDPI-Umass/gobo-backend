import logging
from flask import request, g
import http_errors
import models
from platform_models import bluesky, reddit, mastodon
from .helpers import parse_base_url


def action_onboard_identity_start_post():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    base_url = parse_base_url(request.json)

    if base_url == bluesky.BASE_URL:
        response = bluesky.get_redirect_url(person)
    elif base_url == reddit.BASE_URL:
        response = reddit.get_redirect_url(person)
    else:
        response = mastodon.get_redirect_url(person, base_url)

    return response
  

def action_onboard_identity_callback_post():
    authority_id = g.claims["sub"]
    person = models.person.lookup(authority_id)
    base_url = parse_base_url(request.json)
    
    registration = models.registration.find({
        "person_id": person["id"],
        "base_url": base_url
    })

    if registration == None:
        raise http_errors.unprocessable_content(
            "this person has no pending registration with this provider"
        )


    if base_url == bluesky.BASE_URL:
        data = bluesky.validate_callback(request.json)
        identity = bluesky.confirm_identity(registration, data)
    elif base_url == reddit.BASE_URL:
        data = reddit.validate_callback(request.json)
        identity = reddit.confirm_identity(registration, data)
    else:
        data = mastodon.validate_callback(request.json, base_url)
        identity = mastodon.confirm_identity(registration, data)

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

    base_url = identity["base_url"]

    if base_url == bluesky.BASE_URL:
        queue = "bluesky"
    elif base_url == reddit.BASE_URL:
        queue = "reddit"
    else:
        queue = "mastodon"

    task = models.task.add({
      "queue": queue,
      "name": "pull sources",
      "details": {
        "identity": identity
      }
    })


    return task


def action_workbench_post():
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

    base_url = identity["base_url"]

    if base_url == bluesky.BASE_URL:
        queue = "bluesky"
    elif base_url == reddit.BASE_URL:
        queue = "reddit"
    else:
        queue = "mastodon"

    task = models.task.add({
      "queue": queue,
      "name": "workbench",
      "details": {
        "identity": identity
      }
    })


    return task