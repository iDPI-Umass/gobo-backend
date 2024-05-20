import logging
import models
import joy
import queues
from clients import Linkedin
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
    if task.name == "unpublish post":
        return unpublish_post(task)

    logging.warning("No matching job for task: %s", task)


@tasks.handle_stale
@tasks.handle_delivery
def create_post(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    metadata = task.details.get("metadata", {})

    if len(post["attachments"]) > 20:
        raise Exception("linkedin posts are limited to 20 attachments.")
    for file in post["attachments"]:
        file["data"] = h.read_draft_file(file)

    if metadata.get("link_card_draft_image") is not None:
        file = metadata["link_card_draft_image"]
        file["data"] = h.read_draft_file(file)

    client = Linkedin(identity)
    client.login()
    response = client.create_post(post, metadata)
    logging.info("linkedin: create post complete")
    return {"reference": response["id"]}


@tasks.handle_stale
@tasks.handle_unpublish
def unpublish_post(task):
    identity = h.enforce("identity", task)
    target = h.enforce("target", task)
    reference = target["stash"]["reference"]

    client = Linkedin(identity)
    client.login()
    client.remove_post(reference)    
    logging.info("linkedin: unpublish post complete")