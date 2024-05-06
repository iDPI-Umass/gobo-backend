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

    logging.warning("No matching job for task: %s", task)



@tasks.handle_stale
def create_post(task):
    identity = h.enforce("identity", task)
    post = h.enforce("post", task)
    metadata = task.details.get("metadata", {})

    if len(post["attachments"]) > 20:
        raise Exception("linkedin posts are limited to 20 attachments.")
    for draft in post["attachments"]:
        draft["data"] = h.read_draft_file(draft)

    if metadata.get("link_card_draft_image") is not None:
        draft = metadata["link_card_draft_image"]
        draft["data"] = h.read_draft_file(draft)

    client = Linkedin(identity)
    client.login()
    client.create_post(post, metadata)
    logging.info("linkedin: create post complete")
    for draft in post["attachments"]:
        models.draft_file.publish(draft["id"])
    if metadata.get("link_card_draft_image") is not None:
        draft = metadata.get("link_card_draft_image")
        models.draft_file.publish(draft["id"])
