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
    post_data = h.enforce("post_data", task)
    graph["posts"] = post_data["posts"]
    notifications = client.map_notifications(graph)
    return {"notifications": notifications}


def upsert_notifications(task):
    _notifications = h.enforce("notifications", task)
    notifications = []
    for _notification in _notifications:
        notification = models.notification.upsert(_notification)
        notifications.append(notification)
    return {"notifications": notifications}
