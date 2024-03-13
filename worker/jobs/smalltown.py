import logging
import models
import joy
from clients import Smalltown
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
        
    
    if task.name == "dismiss notification":
        return tasks.dismiss_notification(task)

    
    logging.warning("No matching job for task: %s", task)
    


def create_post(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    metadata = task.details.get("metadata", {})

    for draft in post["attachments"]:
        draft["data"] = h.read_draft_file(draft)

    client = Smalltown(identity)
    client.login()
    client.create_post(post, metadata)
    logging.info("smalltown: create post complete")
    for draft in post["attachments"]:
        draft["published"] = True
        models.draft_image.update(draft["id"], draft)


def add_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    edge = h.enforce("edge", task)
    client = Smalltown(identity)
    client.login()

    if name in ["like", "repost"]:
        if name == "like":
            client.favourite_post(post)
            models.post_edge.add(edge)
            logging.info(f"smalltown: like post complete on {post['id']}")
        elif name == "repost":
            client.boost_post(post)
            models.post_edge.add(edge)
            logging.info(f"smalltown: repost post complete on {post['id']}")
    else:
        raise Exception(
            f"smalltown does not have post edge action defined for {name}"
        )


def remove_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    edge = h.enforce("edge", task)
    client = Smalltown(identity)
    client.login()

    if name in ["like", "repost"]:
        if name == "like":
            client.undo_favourite_post(post)
            models.post_edge.remove(edge["id"])
            logging.info(f"smalltown: undo like post complete on {post['id']}")
        elif name == "repost":
            client.undo_boost_post(post)
            models.post_edge.remove(edge["id"])
            logging.info(f"smalltown: undo repost post complete on {post['id']}")
    else:
        raise Exception(
            f"smalltown does not have post edge action defined for {name}"
        )
