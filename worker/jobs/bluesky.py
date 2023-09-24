import logging
import models
import joy
from clients import Bluesky
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
    elif task.name == "cycle refresh token":
        cycle_refresh_token(task)
    elif task.name == "cycle access token":
        cycle_access_token(task)
    
    
    elif task.name == "bootstrap sessions":
        bootstrap_sessions(task)
    else:
        logging.warning("No matching job for task: %s", task)
    
    task.remove()





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
            models.post_edge.update(edge["id"], edge)
            logging.info(f"bluesky: like post complete on {post['id']}")
        elif name == "repost":
            repost_edge = client.repost_post(post)
            edge["stash"] = repost_edge
            models.post_edge.update(edge["id"], edge)
            logging.info(f"bluesky: repost post complete on {post['id']}")
    else:
        raise logging.warning(
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
            logging.info(f"bluesky: undo like post complete on {post['id']}")
        elif name == "repost":
            client.undo_repost_post(edge)
            logging.info(f"bluesky: undo repost post complete on {post['id']}")
    else:
        raise logging.warning(
            f"bluesky does not have post edge action defined for {name}"
        )
    


def cycle_refresh_token(task):
    session = h.enforce("session", task)
    identity = h.enforce("identity", task)
    _session = Bluesky.create_session(identity)
    _session = Bluesky.map_session(identity, _session)
    models.bluesky_session.update(session["id"], _session)


def cycle_access_token(task):
    session = h.enforce("session", task)
    identity = h.enforce("identity", task)
    _session = Bluesky.refresh_session(session)
    _session = Bluesky.map_session(identity, _session)
    models.bluesky_session.update(session["id"], _session)


def bootstrap_sessions(task):
    identities = QueryIterator(
        model = models.identity,
        wheres = [
            where("base_url", Bluesky.BASE_URL)
        ]
    )

    for identity in identities:
        session = Bluesky.create_session(identity)
        session = Bluesky.map_session(identity, session)
        models.bluesky_session.upsert(session)