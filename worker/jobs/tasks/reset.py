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
    queues.default.put_details("clear post origins", task.details)
    queues.default.put_details("clear post targets", task.details)



def clear_posts(task):
    platform = h.get_platform(task.details)
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]
        
   
    posts = QueryIterator(
        model = models.post,
        for_removal = True,
        wheres = wheres
    )
    for post in posts:
        models.post.remove(post["id"])

def clear_post_origins(task):
    links = QueryIterator(
        model = models.link,
        for_removal = True,
        wheres = [
            where("origin_type", "post")
        ]
    ) 
    for link in links:
        models.link.remove(link["id"])

def clear_post_targets(task):
    # links = QueryIterator(
    #     model = models.link,
    #     for_removal = True,
    #     wheres = [
    #         where("target_type", "post")
    #     ]
    # ) 
    # for link in links:
    #     models.link.remove(link["id"])

    links = QueryIterator(
        model = models.link,
        wheres = [
            where("target_type", "post")
        ]
    )
    ids = []
    for link in links:
        ids.append(link["id"])

    for id in ids:
        queues.default.put_details("clear post target", {"id": id})
        

def clear_post_target(task):
    id = task.details["id"]
    models.link.remove(id)    

        


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