import logging
import models
import queues
import joy
from clients import Bluesky, HTTPError
from . import tasks

h = tasks.helpers
where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def dispatch(task):
    if task.name == "get client":
        return tasks.get_client(task)
    if task.name == "get profile":
        return tasks.get_profile(task)
    
    
    if task.name == "pull sources":
        return tasks.pull_sources(task)
    if task.name == "pull posts":
        return tasks.pull_posts(task)
    if task.name == "pull notifications":
        return tasks.pull_notifications(task)
    
    
    if task.name == "create post":
        return create_post(task)
    if task.name == "add post edge":
       return add_post_edge(task)
    if task.name == "remove post edge":
        return remove_post_edge(task)
    if task.name == "cycle refresh token":
        return cycle_refresh_token(task)
    if task.name == "cycle access token":
        return cycle_access_token(task)


    if task.name == "dismiss notification":
        return tasks.dismiss_notification(task)


    logging.warning("No matching job for task: %s", task)



def create_post(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    metadata = task.details.get("metadata", {})

    for draft in post["attachments"]:
        draft["data"] = h.read_draft_file(draft)

    client = Bluesky(identity)
    client.login()
    client.create_post(post, metadata)
    logging.info("bluesky: create post complete")
    for draft in post["attachments"]:
        draft["published"] = True
        models.draft_image.update(draft["id"], draft)



def add_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    edge = h.enforce("edge", task)
    client = Bluesky(identity)
    client.login()

    if name in ["like", "repost"]:
        if name == "like":
            like_edge = client.like_post(post)
            edge["stash"] = like_edge
            models.post_edge.add(edge)
            logging.info(f"bluesky: like post complete on {post['id']}")
        elif name == "repost":
            repost_edge = client.repost_post(post)
            edge["stash"] = repost_edge
            models.post_edge.add(edge)
            logging.info(f"bluesky: repost post complete on {post['id']}")
    else:
        raise Exception(
            f"bluesky does not have post edge action defined for {name}"
        )

def remove_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    edge = h.enforce("edge", task)
    client = Bluesky(identity)
    client.login()

    if name in ["like", "repost"]:
        if name == "like":
            client.undo_like_post(edge)
            models.post_edge.remove(edge["id"])
            logging.info(f"bluesky: undo like post complete on {post['id']}")
        elif name == "repost":
            client.undo_repost_post(edge)
            models.post_edge.remove(edge["id"])
            logging.info(f"bluesky: undo repost post complete on {post['id']}")
    else:
        raise Exception(
            f"bluesky does not have post edge action defined for {name}"
        )