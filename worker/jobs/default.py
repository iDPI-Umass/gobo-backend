import logging
import models
from . import tasks

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def dispatch(task):
    if task.name == "fanout - update identity":
        return tasks.fanout_update_identity(task)
    if task.name == "fanout - pull notifications":
        return tasks.fanout_pull_notifications(task)


    if task.name == "start flow":
        return tasks.start_flow(task)
    if task.name == "flow - update identity":
        return tasks.flow_update_identity(task)
    if task.name == "flow - update identity feed":
        return tasks.flow_update_identity_feed(task)
    if task.name == "flow - pull sources":
        return tasks.flow_pull_sources(task)
    if task.name == "flow - pull posts":
        return tasks.flow_pull_posts(task)
    if task.name == "flow - pull notifications":
        return tasks.flow_pull_notifications(task)
    if task.name == "flow - dismiss notification":
        return tasks.flow_dismiss_notification(task)


    if task.name == "get profile":
        return tasks.get_profile(task)
    if task.name == "map profile":
        return tasks.map_profile(task)
    if task.name == "upsert profile":
        return tasks.upsert_profile(task)


    if task.name == "follow":
        return tasks.follow(task)
    if task.name == "unfollow":
        return tasks.unfollow(task)
    if task.name == "remove identity":
        return tasks.remove_identity(task)


    if task.name == "map sources":
        return tasks.map_sources(task)
    if task.name == "upsert sources":
        return tasks.upsert_sources(task)
    if task.name == "reconcile sources":
        return tasks.reconcile_sources(task)
    if task.name == "remove source":
        return tasks.remove_source(task)


    if task.name == "check source lockout":
        return tasks.check_source_lockout(task)
    if task.name == "get source cursor":
        return tasks.get_source_cursor(task)
    if task.name == "pull posts from source":
        return tasks.pull_posts_from_source(task)
    if task.name == "map posts":
        return tasks.map_posts(task)
    if task.name == "upsert posts":
        return tasks.upsert_posts(task)


    if task.name == "add post to followers":
        return tasks.add_post_to_followers(task)
    if task.name == "add post to list followers":
        return tasks.add_post_to_list_followers(task)
    if task.name == "remove post":
        return tasks.remove_post(task)
    if task.name == "rebuild feed":
        return tasks.rebuild_feed(task)
    

    if task.name == "get notification cursor":
        return tasks.get_notification_cursor(task)
    if task.name == "map notifications":
        return tasks.map_notifications(task)
    if task.name == "upsert notifications":
        return tasks.upsert_notifications(task)


    if task.name == "hard reset":
        return tasks.hard_reset(task)
    if task.name == "clear posts":
        return tasks.clear_posts(task)
    if task.name == "clear sources":
        return tasks.clear_sources(task)
    if task.name == "clear cursors":
        return tasks.clear_cursors(task)
    if task.name == "clear counters":
        return tasks.clear_counters(task)
    if task.name == "clear notifications":
        return tasks.clear_notifications(task)
    if task.name == "clear notification cursors":
        return tasks.clear_notification_cursors(task)


    if task.name == "bluesky cycle sessions":
        return tasks.cycle_bluesky_sessions(task)


    if task.name == "prune resources":
        return tasks.prune_resources(task)
    if task.name == "prune draft images":
        return tasks.prune_draft_images(task)  
    if task.name == "prune posts":
        return tasks.prune_posts(task)
    if task.name == "prune registrations":
        return tasks.prune_registrations(task)
    if task.name == "prune sources":
        return tasks.prune_sources(task)
    if task.name == "prune notifications":
        return tasks.prune_notifications(task)


    if task.name == "workbench":
        return tasks.workbench(task)
    

    if task.name == "label identity platform":
        return tasks.label_identity_platform(task)
    if task.name == "label post platform":
        return tasks.label_post_platform(task)
    if task.name == "label registration platform":
        return tasks.label_registration_platform(task)
    if task.name == "label source platform":
        return tasks.label_source_platform(task)


    if task.name == "test":
        return tasks.test(task)
    
    logging.warning("No matching job for task: %s", task)