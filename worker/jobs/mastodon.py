import logging
import models
import joy
from clients import Mastodon
from . import tasks

h = tasks.helpers
where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def dispatch(task):
    if task.name == "pull sources":
        return tasks.pull_sources(task)
    elif task.name == "pull posts":
        return tasks.pull_posts(task)
    
    
    elif task.name == "create post":
        return create_post(task)
    elif task.name == "add post edge":
        return add_post_edge(task)
    elif task.name == "remove post edge":
        return remove_post_edge(task)


    else:
        logging.warning("No matching job for task: %s", task)
    


def create_post(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    metadata = task.details.get("metadata", {})

    for draft in post["attachments"]:
        draft["data"] = h.read_draft_file(draft)

    client = Mastodon(identity)
    client.login()
    client.create_post(post, metadata)
    logging.info("mastodon: create post complete")
    for draft in post["attachments"]:
        draft["published"] = True
        models.draft_image.update(draft["id"], draft)


def add_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    client = Mastodon(identity)
    client.login()

    if name in ["like", "repost"]:
        if name == "like":
            client.favourite_post(post)
            logging.info(f"mastodon: like post complete on {post['id']}")
        elif name == "repost":
            client.boost_post(post)
            logging.info(f"mastodon: repost post complete on {post['id']}")
    else:
        raise logging.warning(
            f"mastodon does not have post edge action defined for {name}"
        )


def remove_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    client = Mastodon(identity)
    client.login()

    if name in ["like", "repost"]:
        if name == "like":
            client.undo_favourite_post(post)
            logging.info(f"mastodon: undo like post complete on {post['id']}")
        elif name == "repost":
            client.undo_boost_post(post)
            logging.info(f"mastodon: undo repost post complete on {post['id']}")
    else:
        raise logging.warning(
            f"mastodon does not have post edge action defined for {name}"
        )
