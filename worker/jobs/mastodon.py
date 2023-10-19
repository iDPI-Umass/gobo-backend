import logging
import models
import joy
import queues
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
        try:
            return create_post(task)
        except Exception as e:
            logging.error(e)
            raise Exception("mastodon create post failure")
    elif task.name == "add post edge":
        try:
            return add_post_edge(task)
        except Exception as e:
            logging.error(e)
            raise Exception("mastodon add post edge failure")
    elif task.name == "remove post edge":
        try:
            return remove_post_edge(task)
        except Exception as e:
            logging.error(e)
            raise Exception("mastodon remove post edge failure")


    else:
        logging.warning("No matching job for task: %s", task)


# Special case. Because Mastodon is federated we need to pull multiple copies of
# resources and treat each one as a mini platform. But we can also split the
# load for throttled actions for large servers.
def super_dispatch(task):
    identity = task.details.get("identity", None)
    if identity is None:
        queues.mastodon_default.put_task(task)
    elif identity["base_url"] == "https://mastodon.social":
        queues.mastodon_social.put_task(task)
    elif identity["base_url"] == "https://hachyderm.io":
        queues.mastodon_hachyderm.put_task(task)
    elif identity["base_url"] == "https://octodon.social":
        queues.mastodon_octodon.put_task(task)
    elif identity["base_url"] == "https://techpolicy.social":
        queues.mastodon_techpolicy.put_task(task)
    elif identity["base_url"] == "https://vis.social":
        queues.mastodon_vis_social.put_task(task)
    elif identity["base_url"] == "https://social.coop":
        queues.mastodon_social_coop.put_task(task)
    else:
        queues.mastodon_default.put_task(task)
    


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
