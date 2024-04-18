import logging
import joy
import models
import queues
from . import helpers as h

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def hard_reset(task):
    queues.default.put_details(
        name = "clear posts",
        priority = task.priority,
        details = task.details
    )
    queues.default.put_details(
        name = "clear sources",
        priority = task.priority,
        details = task.details
    )
    queues.default.put_details(
        name = "clear notifications",
        priority = task.priority,
        details = task.details
    )
    queues.default.put_details(
        name = "clear cursors",
        priority = task.priority,
        details = task.details
    )
    queues.default.put_details(
        name = "clear counters",
        priority = task.priority,
        details = task.details
    )



def clear_posts(task):
    platform = h.get_platform(task.details)
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]
    
    max_id = None
    while True:
        _wheres = wheres.copy()
        if max_id is not None:
            _wheres.append(where("id", max_id, "gt"))
        
        posts = models.post.scan({
            "direction": "ascending",
            "where": _wheres
        })

        if len(posts) == 0:
            break
        
        max_id = posts[-1]["id"]
        for post in posts:
            queues.default.put_details(
                name = "remove post",
                priority = task.priority,
                details = {"post": post}
            )


def clear_sources(task):
    platform = h.get_platform(task.details)
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]
    
    max_id = None
    while True:
        _wheres = wheres.copy()
        if max_id is not None:
            _wheres.append(where("id", max_id, "gt"))
        
        sources = models.source.scan({
            "direction": "ascending",
            "where": _wheres
        })

        if len(sources) == 0:
            break
        
        max_id = sources[-1]["id"]
        for source in sources:
            queues.default.put_details(
                name = "remove source",
                priority = task.priority,
                details = {"source": source}
            )


# TODO: support platform-specific action here like the others.
def clear_notifications(task):    
    notifications = QueryIterator(
        model = models.notification,
        for_removal = True,
    )
    for notification in notifications:
        h.remove_notification(notification)
        
    links = QueryIterator(
        model = models.link,
        for_removal = True,
        wheres = [
            where("name", "read-cursor-notification")
        ]
    )
    for link in links:
        models.link.remove(link["id"])

def clear_notification_cursors(task):            
    links = QueryIterator(
        model = models.link,
        for_removal = True,
        wheres = [
            where("name", "read-cursor-notification")
        ]
    )
    for link in links:
        models.link.remove(link["id"])



def clear_cursors(task):
    platform = h.get_platform(task.details)
    wheres = [
        where("name", "read-cursor-", "starts with")
    ]
    
    if platform == "all":
        links = QueryIterator(
            model = models.link,
            for_removal = True,
            wheres = wheres
        ) 
        for link in links:
            models.link.remove(link["id"])
    else:
        links = QueryIterator(
            model = models.link,
            wheres = wheres
        ) 
        removals = []
        for link in links:
            source = models.source.get(link["origin_id"])
            if source is not None and source.get("platform") == platform:
                removals.append(link["id"])


        for id in removals:
            models.link.remove(id)


def clear_counters(task):
    wheres = []
    
    counters = QueryIterator(
        model = models.counter,
        for_removal = True,
        wheres = wheres
    ) 
    for counter in counters:
        models.counter.remove(counter["id"])