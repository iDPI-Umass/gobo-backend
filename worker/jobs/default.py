import logging
import models
from . import tasks

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def dispatch(task):
    if task.name == "start flow":
        return tasks.start_flow(task)
    elif task.name == "flow - pull sources":
        return tasks.flow_pull_sources(task)
    elif task.name == "flow - pull posts":
        return tasks.flow_pull_posts(task)
    elif task.name == "flow - onboard sources":
        return tasks.flow_onboard_sources(task)
    elif task.name == "flow - onboard source posts":
        return tasks.flow_onboard_source_posts(task)


    elif task.name == "follow":
        return tasks.follow(task)
    elif task.name == "unfollow":
        return tasks.unfollow(task)
    elif task.name == "remove identity":
        return tasks.remove_identity(task)


    elif task.name == "pull sources fanout":
        return tasks.pull_sources_fanout(task)
    elif task.name == "map sources":
        return tasks.map_sources(task)
    elif task.name == "upsert sources":
        return tasks.upsert_sources(task)
    elif task.name == "reconcile sources":
        return tasks.reconcile_sources(task)

    
    elif task.name == "pull posts fanout":
        return tasks.pull_posts_fanout(task)
    elif task.name == "get last retrieved":
        return tasks.get_last_retrieved(task)
    elif task.name == "set last retrieved":
        return tasks.set_last_retrieved(task)
    elif task.name == "map posts":
        return tasks.map_posts(task)
    elif task.name == "upsert posts":
        return tasks.upsert_posts(task)


    elif task.name == "add post to followers":
        return tasks.add_post_to_followers(task)
    elif task.name == "remove post":
        return tasks.remove_post(task)
    elif task.name == "rebuild feed":
        return tasks.rebuild_feed(task)


    elif task.name == "hard reset":
        return tasks.hard_reset(task)
    elif task.name == "clear posts":
        return tasks.clear_posts(task)
    elif task.name == "clear post origins":
        return tasks.clear_post_origins(task)
    elif task.name == "clear post targets":
        return tasks.clear_post_targets(task)
    elif task.name == "clear post target":
        return tasks.clear_post_target(task)
    elif task.name == "clear last retrieved":
        return tasks.clear_last_retrieved(task)


    elif task.name == "bluesky cycle sessions":
        return tasks.cycle_blusky_sessions(task)


    elif task.name == "prune resources":
        return tasks.prune_resources(task)
    elif task.name == "prune draft images":
        return tasks.prune_draft_images(task)  
    elif task.name == "prune posts":
        return tasks.prune_posts(task)
    elif task.name == "prune registrations":
        return tasks.prune_registrations(task)
    elif task.name == "prune sources":
        return tasks.prune_sources(task)


    elif task.name == "workbench":
        return tasks.workbench(task)
    
    
    elif task.name == "bootstrap platform labels":
        return tasks.boostrap_platform_labels(task)
    elif task.name == "label identity platform":
        return tasks.label_identity_platform(task)
    elif task.name == "label post platform":
        return tasks.label_post_platform(task)
    elif task.name == "label registration platform":
        return tasks.label_registration_platform(task)
    elif task.name == "label source platform":
        return tasks.label_source_platform(task)


    elif task.name == "test":
        return tasks.test(task)
    else:
        logging.warning("No matching job for task: %s", task)