import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def get_notification_cursor(task):
    identity = h.enforce("identity", task)    
    name = "read-cursor-notification"
    timeout = 0

    cursor = models.link.LoopCursor("identity", identity["id"], name)
    last_retrieved = cursor.stamp(timeout)

    # If this isn't a viable read, we need to bail.
    if last_retrieved == False:
        task.halt()
        return
    else:
      return {
        "cursor": cursor,
        "last_retrieved": last_retrieved
      }


def pull_notifications(task):
    client = h.enforce("client", task)
    cursor = h.enforce("cursor", task)
    last_retrieved = cursor.last_retrieved
    graph = client.list_notifications({"last_retrieved": last_retrieved})
    return {"graph": graph}


def map_notifications(task):
    client = h.enforce("client", task)
    graph = h.enforce("graph", task)
    graph["sources"] = h.enforce("sources", task)
    graph["posts"] = h.enforce("posts", task)
    notifications = client.map_notifications(graph)
    return {"notifications": notifications}


def upsert_notifications(task):
    identity = h.enforce("identity", task)
    _notifications = h.enforce("notifications", task)
    notifications = []
    for item in _notifications:
        notification = models.notification.upsert(item)
        notifications.append(notification)

        models.link.upsert({
            "origin_type": "identity",
            "origin_id": identity["id"],
            "target_type": "notification",
            "target_id": notification["id"],
            "name": "notification-feed",
            "secondary": f"{notification['notified']}::{notification['id']}"
        })

        if notification["type"] == "mention":
            models.link.upsert({
                "origin_type": "identity",
                "origin_id": identity["id"],
                "target_type": "notification",
                "target_id": notification["id"],
                "name": "notification-mention-feed",
                "secondary": f"{notification['notified']}::{notification['id']}"
            })            

        if notification.get("post_id") is not None:
            models.link.upsert({
                "origin_type": "notification",
                "origin_id": notification["id"],
                "target_type": "post",
                "target_id": notification["post_id"],
                "name": "notifies",
                "secondary": f"{notification['notified']}::{notification['id']}"
            })

    return {"notifications": notifications}


def dismiss_notification(task):
    id = h.enforce("notification_id", task)
    client = h.enforce("client", task)
    
    notification = models.notification.get(id)
    if notification is None:
        logging.warn(f"cannot dismiss notification {id} because it was not found")
    
    client.dismiss_notification(notification)