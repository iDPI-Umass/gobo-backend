import logging
import models
import joy
from clients import Reddit
from . import tasks

h = tasks.helpers
where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def dispatch(task):
    if task.name == "pull sources":
        tasks.pull_sources(task)
    elif task.name == "pull posts":
        tasks.pull_posts(task)
    
    
    elif task.name == "create post":
        create_post(task)
    elif task.name == "add post edge":
        add_post_edge(task)
    elif task.name == "remove post edge":
        remove_post_edge(task)


    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()




def create_post(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    metadata = task.details.get("metadata", {})

    # TODO: Reddit is a little funky because the replies aren't modeled as posts
    #       currently. That might change? For now, special method and return.
    if metadata.get("reply", None) is not None:
        client = Reddit(identity)
        client.login()
        client.create_reply(post, metadata)
        logging.info("reddit: create reply comment complete")
        return

    for draft in post["attachments"]:
        draft["data"] = h.read_draft_file(draft)

    client = Reddit(identity)
    client.login()
    client.create_post(post, metadata)
    logging.info("reddit: create post complete")
    for draft in post["attachments"]:
        draft["published"] = True
        models.draft_image.update(draft["id"], draft)


def add_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    client = Reddit(identity)
    client.login()

    if name in ["upvote", "downvote"]:
        if name == "upvote":
            client.upvote_post(post)
            logging.info(f"reddit: upvote post complete on {post['id']}")
        elif name == "downvote":
            client.downvote_post(post)
            logging.info(f"reddit: dowvote post complete on {post['id']}")
    else:
        raise logging.warning(
            f"reddit does not have post edge action defined for {name}"
        )

def remove_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    client = Reddit(identity)
    client.login()

    if name in ["upvote", "downvote"]:
        if name in ["upvote", "downvote"]:
            client.undo_vote_post(post)
            logging.info(f"reddit: undo vote post complete on {post['id']}")
    else:
        raise logging.warning(
            f"reddit does not have post edge action defined for {name}"
        )