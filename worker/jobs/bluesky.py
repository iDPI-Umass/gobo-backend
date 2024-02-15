import logging
import models
import queues
import joy
from clients import Bluesky, HTTPError
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
        try:
            return create_post(task)
        except Exception as e:
            logging.error(e)
            raise Exception("bluesky create post failure")
    if task.name == "add post edge":
        try:
            return add_post_edge(task)
        except Exception as e:
            logging.error(e)
            raise Exception("bluesky add post edge failure")
    if task.name == "remove post edge":
        try:
            return remove_post_edge(task)
        except Exception as e:
            logging.error(e)
            raise Exception("bluesky remove post edge failure")
    if task.name == "cycle refresh token":
        return cycle_refresh_token(task)
    if task.name == "cycle access token":
        return cycle_access_token(task)
    
    
    if task.name == "bootstrap sessions":
        return bootstrap_sessions(task)
    
    
    if task.name == "dismiss notification":
        return tasks.dismiss_notification(task)


    logging.warning("No matching job for task: %s", task)



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



# TODO: Gobo proxies an authorized connection to Bluesky as though it were a 
#     frontend client. We need to maintain our authorization to the Bluesky
#     account by cycling out tokens. If the Bluesky member revokes their
#     authorization, we need to detect that and remove the identity from Gobo.
#
#     The tricky part is that we need to map Bluesky token errors into some sort
#     of action at the Gobo identity level. There's an interface question where
#     we handle all Bluesky client errors of this type when we attempt to make
#     a request. But failure flow logic is somewhat harder to express right now.
#     So, as a compromise, we'll handle it here. It complicates the legibility
#     of these tasks, but these cycle regularly and only trigger on very specific
#     error conditions from the client. So the risk is contained.


def cycle_refresh_token(task):
    session = h.enforce("session", task)
    identity = h.enforce("identity", task)
    try:
        _session = Bluesky.create_session(identity)
    except HTTPError as e:
        if e.status == 400:
            error = e.body.get("error")
            if error == "ExpiredToken":
                models.bluesky_session.remove(session["id"])
                queues.default.put_details(
                    priority = 1,
                    name = "remove identity",
                    details = {"identity": identity}
                )
                return
    
    _session = Bluesky.map_session(identity, _session)
    models.bluesky_session.update(session["id"], _session)


def cycle_access_token(task):
    session = h.enforce("session", task)
    identity = h.enforce("identity", task)
    try:
        _session = Bluesky.refresh_session(session)
    except HTTPError as e:
        if e.status == 400:
            error = e.body.get("error")
            if error == "ExpiredToken":
                models.bluesky_session.remove(session["id"])
                queues.default.put_details(
                    priority = 1,
                    name = "remove identity",
                    details = {"identity": identity}
                )
                return
    
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