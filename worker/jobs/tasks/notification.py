import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator

class NotificationCursor:
    def __init__(self, id):
        self.link = models.notification.get_cursor(id)
    
    # This is a provisional update we make optimistically.
    # We need to reverse this if we don't get a successful fetch and map.
    def stamp(self):
        now = joy.time.now()
        self.update(now)
        self.last_retrieved = self.link.get("secondary", now)
        return self.last_retrieved

    def update(self, time):
        link = self.link
        link["secondary"] = time
        models.link.update(link["id"], link)
  
    # We detected a failure and need to roll back the timestamp
    def rollback(self):
        self.update(self.last_retrieved)


def get_notification_cursor(task):
    identity = h.enforce("identity", task)
    cursor = NotificationCursor(identity["id"])
    cursor.stamp()
    return {"cursor": cursor}


def pull_notifications(task):
    client = h.enforce("client", task)
    cursor = h.enforce("cursor", task)
    graph = client.list_notifications(
        last_retrieved = cursor.last_retrieved
    )
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
