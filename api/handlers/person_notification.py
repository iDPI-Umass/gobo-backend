import logging
from flask import request
import http_errors
import models

valid_views = ["all", "mentions"]

def parse_feed_query():
    data = request.args

    try:
        per_page = int(data.get("per_page") or 25)
    except Exception as e:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")

    if per_page < 1:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")
    if per_page > 100:
        per_page = 100

    start = data.get("start")

    view = data.get("view", "all")
    if view not in valid_views:
        raise http_errors.bad_request(f"view {view} is invalid")
    if view == "all":
        view = "notification-feed"
    elif view == "mentions":
        view = "notification-mention-feed"

    return {
      "per_page": per_page,
      "start": start,
      "view": view
    }

def person_notifications_get(person_id, id):
    query = parse_feed_query()
    query["person_id"] = person_id
    query["identity_id"] = id

    identity = models.identity.get(id)
    if identity == None or identity["person_id"] != person_id:
        raise http_errors.not_found(
            f"person notifications /people/{person_id}/identities/{id}/notifications is not found"
        )


    return {"content": models.notification.view_identity_feed(query)}


# This serves to mark the notification as read in the Gobo graph, and for the
# provider platform where supported. This does not delete the notification.
def person_notification_post(person_id, identity_id, id):
    # Confirm this identity actually belongs to the person in question.
    identity = models.identity.get(identity_id)
    if identity == None or identity["person_id"] != person_id:
        raise http_errors.not_found(
            f"person notification /people/{person_id}/identities/{identity_id}/notifications/{id} is not found"
        )
    
    # Confirm this notification actually belongs to the identity in question.
    edge = models.link.find({
        "origin_type": "identity",
        "origin_id": identity["id"],
        "target_type": "notification",
        "target_id": id,
        "name": "notification-feed"
    })

    if edge is None:
        raise http_errors.not_found(
            f"person notification /people/{person_id}/identities/{identity_id}/notifications/{id} is not found"
        )   

    notification = models.notification.get(id)
    if notification is None:
        raise http_errors.not_found(
            f"person notification /people/{person_id}/identities/{identity_id}/notifications/{id} is not found"
        )


    # Update the Gobo resource to indicate the notification is read.
    notification["active"] = False
    models.notification.update(id, notification)
    return {"content": notification}






    
