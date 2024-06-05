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
    thread = h.enforce("thread", task)

    client = Linkedin(identity)
    client.login()

    # While this always uses the generic thread structure, we don't currently
    # support going beyond a single post.
    references = []
    post = thread[0]
    metadata = post["metadata"]

    if len(post["attachments"]) > 20:
        raise Exception("linkedin posts are limited to 20 attachments.")
    for file in post["attachments"]:
        file["data"] = h.read_draft_file(file)

    if metadata.get("link_card_draft_image") is not None:
        file = metadata["link_card_draft_image"]
        file["data"] = h.read_draft_file(file)

    result = client.create_post(post, metadata)
    logging.info("linkedin: create post complete")    

    references.append({
        "reference": result["id"],
        "url": ""
    })

    return references



@tasks.handle_stale
@tasks.handle_unpublish
def unpublish_post(task):
    identity = h.enforce("identity", task)
    target = h.enforce("target", task)
    references = target["stash"]["references"]

    client = Linkedin(identity)
    client.login()
    for item in references:
        client.remove_post(item["reference"])
    logging.info("linkedin: unpublish post complete")