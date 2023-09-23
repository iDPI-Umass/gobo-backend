import logging
import models
from . import tasks

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def dispatch(task):
    if task.name == "start flow":
        tasks.start_flow(task)
    if task.name == "flow pull sources":
        tasks.flow_pull_sources(task)
    if task.name == "flow pull posts":
        tasks.flow_pull_posts(task)
    if task.name == "flow onboard sources":
        tasks.flow_onboard_sources(task)
    if task.name == "flow onboard source posts":
        tasks.flow_onboard_source_posts(task)


    if task.name == "follow":
        tasks.follow(task)
    elif task.name == "unfollow":
        tasks.unfollow(task)
    elif task.name == "remove identity":
        tasks.remove_identity(task)


    elif task.name == "add post to followers":
        tasks.add_post_to_followers(task)
    elif task.name == "remove post":
        tasks.remove_post(task)
    elif task.name == "rebuild feed":
        tasks.rebuild_feed(task)


    elif task.name == "hard reset":
        tasks.hard_reset(task)
    elif task.name == "clear posts":
        tasks.clear_posts(task)
    elif task.name == "clear last retrieved":
        tasks.clear_last_retrieved(task)


    elif task.name == "prune image cache":
        tasks.prune_image_cache(task)  


    elif task.name == "workbench":
        tasks.workbench(task)


    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()