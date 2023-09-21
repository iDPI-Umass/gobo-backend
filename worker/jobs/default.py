import logging
import models
from . import tasks

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def dispatch(task):
    task.handler = "database"
    logging.info("dispatching: %s", task)

    if task.name == "follow":
        tasks.follow(task)
    elif task.name == "unfollow":
        tasks.unfollow(task)
    elif task.name == "remove identity":
        tasks.remove_identity(task)


    elif task.name == "add post to source":
        tasks.add_post_to_source(task)
    elif task.name == "add partial post":
        tasks.add_partial_post(task)
    elif task.name == "add interpost edge":
        tasks.add_interpost_edge(task)
    elif task.name == "add post to followers":
        tasks.add_post_to_followers(task)
    elif task.name == "remove post":
        tasks.remove_post(task)
    elif task.name == "rebuild feed":
        tasks.rebuild_feed(task)


    elif task.name == "hard reset":
        tasks.hard_reset(task)
    elif task.name == "remove posts":
        tasks.remove_posts(task)
    elif task.name == "remove last retrieved":
        tasks.remove_last_retrieved(task)


    elif task.name == "prune image cache":
        tasks.prune_image_cache(task)  


    elif task.name == "workbench":
        tasks.workbench(task)


    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()