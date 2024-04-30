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
    if task.name == "get client":
        return tasks.get_client(task)
    if task.name == "get profile":
        return tasks.get_profile(task)

    
    if task.name == "create post":
        return create_post(task)

    logging.warning("No matching job for task: %s", task)




def create_post(task):
    pass
    # identity = h.enforce("identity", task)
    # post = h.enforce("post", task)
    # metadata = task.details.get("metadata", {})

    # for draft in post["attachments"]:
    #     draft["data"] = h.read_draft_file(draft)

    # client = Mastodon(identity)
    # client.login()
    # client.create_post(post, metadata)
    # logging.info("mastodon: create post complete")
    # for draft in post["attachments"]:
    #     models.draft_image.publish(draft["id"])
