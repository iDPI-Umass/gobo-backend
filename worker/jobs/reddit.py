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
        models.draft_image.publish(draft["id"])


def add_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    edge = h.enforce("edge", task)
    client = Reddit(identity)
    client.login()

    if name in ["upvote", "downvote"]:
        if name == "upvote":
            client.upvote_post(post)
            models.post_edge.add(edge)
            logging.info(f"reddit: upvote post complete on {post['id']}")
        elif name == "downvote":
            client.downvote_post(post)
            models.post_edge.add(edge)
            logging.info(f"reddit: dowvote post complete on {post['id']}")
    else:
        raise Exception(
            f"reddit does not have post edge action defined for {name}"
        )

def remove_post_edge(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    name = h.enforce("name", task)
    edge = h.enforce("edge", task)
    client = Reddit(identity)
    client.login()

    if name in ["upvote", "downvote"]:
        if name in ["upvote", "downvote"]:
            client.undo_vote_post(post)
            models.post_edge.remove(edge["id"])
            logging.info(f"reddit: undo vote post complete on {post['id']}")
    else:
        raise Exception(
            f"reddit does not have post edge action defined for {name}"
        )