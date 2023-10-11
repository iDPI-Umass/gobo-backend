import logging
import joy
import models
import queues
from . import helpers as h

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def hard_reset(task):
    queues.default.put_details("clear posts", task.details)
    queues.default.put_details("clear last retrieved", task.details)



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
            queues.default.put_details("remove post", {"post": post})


def clear_last_retrieved(task):
    platform = h.get_platform(task.details)
    wheres = [
        where("origin_type", "source"),
        where("name", "last-retrieved"),
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