import logging
import models
import joy
from clients import Bluesky, Reddit, Mastodon
import queues
from .helpers import is_valid_platform

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def follow_fanout(task):
    platform = task.details.get("platform", None)
    if not is_valid_platform(platform):
        raise Exception(f"clear posts does not support platform {platform}")
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]

    identities = QueryIterator(
        model = models.identity,
        wheres = wheres
    )
    for identity in identities:
        queues.default.put_details("pull sources", {"identity": identity})


def reconcile_sources(identity, sources):
    desired_sources = []
    for source in sources:
        desired_sources.append(source["id"])
    
    results = models.link.pull([
        where("origin_type", "identity"),
        where("origin_id", identity["id"]),
        where("target_type", "source"),
        where("name", "follows")
    ])
   
    current_sources = []
    source_ids = [ result["target_id"] for result in results ]
    for source in models.source.pluck(source_ids):
        if source["base_url"] == identity["base_url"]:
            current_sources.append(source["id"])


    difference = list(set(desired_sources) - set(current_sources))
    for source_id in difference:
        logging.info(f"For identity {identity['id']}, adding source {source_id}")
        queues.default.put_details("follow", {
            "identity_id": identity["id"],
            "source_id": source_id
        })

    difference = list(set(current_sources) - set(desired_sources))
    for source_id in difference:
        logging.info(f"For identity {identity['id']}, removing source {source_id}")
        queues.default.put_details("unfollow", {
            "identity_id": identity["id"],
            "source_id": source_id
        })


def pull_sources(task):
    identity = task.details.get("identity")
    if identity == None:
        raise Exception("pull posts task requires an identity to run")
    platform = identity["platform"]
    if not is_valid_platform(platform):
        raise Exception(f"pull sources does not support platform {platform}")

    if platform == "bluesky":
        client = Bluesky(identity)
    elif platform == "mastodon":
        base_url = identity["base_url"]
        mastodon_client = models.mastodon_client.find({"base_url": base_url})
        if mastodon_client == None:
            logging.warning(f"no mastodon client found for {base_url}")
            return
        client = Mastodon(mastodon_client, identity)
    elif platform == "reddit":
        client = Reddit(identity)
    else:
        raise Exception("unknown platform")

    data = client.list_sources()
    _sources = client.map_sources(data)

    sources = []
    for _source in _sources:
        source = models.source.upsert(_source)
        sources.append(source)

    reconcile_sources(identity, sources)
    return sources
    




def set_onboard_sources(Client, queue):
    def onboard_sources(task):
        identity = task.details.get("identity")
        if identity == None:
            raise Exception("pull posts task requires an identity to run")
        
        base_url = identity["base_url"]

        if Client.__name__ == "Mastodon":
            mastodon_client = models.mastodon_client.find({"base_url": base_url})
            if mastodon_client == None:
                logging.warning(f"no mastodon client found for {base_url}")
                return
            client = Client(mastodon_client, identity)
        else:
            client = Client(identity)
            

        data = client.list_sources()
        _sources = client.map_sources(data)

        sources = []
        for _source in _sources:
            source = models.source.upsert(_source)
            sources.append(source)
      
        reconcile_sources(identity, sources)
        sources

        for source in sources:
            queue.put_details("pull posts", {
                "client": client,
                "source": source,
                "shallow": True
            })

    return onboard_sources


# TODO: Can we refactor the platform clients so we are not required to look up
#    an identity? Probably not as we move into more private posts. In that case
#    we should refactor some of the primitives here to make this less error-prone
#    to write.
def set_read_sources(where_statements, queue):
    def read_sources(task):
        page = task.details.get("page") or 1
        per_page = 500
        query = {
            "per_page": per_page,
            "page": page,
            "where": where_statements
        }

        sources = models.source.query(query)
        for source in sources:
            queue.put_details( "read source", {"source": source})

        if len(sources) == per_page:
            task.update({"page": page + 1})
            queue.put_task(task)

    return read_sources



def set_read_source(Client, queue):
    def read_source(task):
        source = task.details.get("source")
        if source is None:
            raise Exception("read source: needs source to be specified")

        link = models.link.random([
            where("origin_type", "identity"),
            where("target_type", "source"),
            where("target_id", source["id"]),
            where("name", "follows")
        ])

        # If there is no link, this source is incidential on someone's feed,
        # so it is not directly watched for posts here.
        if link is None:
            return

        identity = models.identity.get(link["origin_id"])
        if identity is None:
            raise Exception(f"no identity found with id {link['origin_id']}")

        base_url = identity["base_url"]

        if Client.__name__ == "Mastodon":
            mastodon_client = models.mastodon_client.find({"base_url": base_url})
            if mastodon_client == None:
                logging.warning(f"no mastodon client found for {base_url}")
                return
            client = Client(mastodon_client, identity)
        else:
            client = Client(identity)

        queue.put_details("pull posts", {
            "client": client,
            "source": source
        })

    return read_source



def set_pull_posts(queue):
    def pull_posts(task):
        client = task.details.get("client")
        source = task.details.get("source")
        is_shallow = task.details.get("shallow", False)
        base_url = source["base_url"]
        if client == None or source == None:
            raise Exception("pull posts task lacks needed inputs")

        link = models.source.get_last_retrieved(source["id"])
        source["last_retrieved"] = link.get("secondary")


        last_retrieved = joy.time.now()
        data = client.get_post_graph(source, is_shallow=is_shallow)
        
        sources = []
        _sources = client.map_sources(data)
        for _source in _sources:
            source = models.source.upsert(_source)
            sources.append(source)
        data["sources"] = sources


        post_data = client.map_posts(data)
        if is_shallow != True:
            link["secondary"] = last_retrieved
            models.link.update(link["id"], link)

        for post in post_data["posts"]:
            queues.default.put_details("add post to source", {
                "post": post
            })
        for post in post_data["partials"]:
            queues.default.put_details("add partial post", {
                "post": post
            })
        for edge in post_data["edges"]:
            queues.default.put_details("add interpost edge", {
                "base_url": base_url,
                "edge_reference": edge
            })           


    return pull_posts

