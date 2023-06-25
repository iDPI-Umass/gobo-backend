import logging
import models
import joy
import queues

where = models.helpers.where


def set_identity_follow_fanout(where_statement, queue):
    def identity_follow_fanout(task):
        page = task.details.get("page") or 1
        per_page = 500
        query = {
            "per_page": per_page,
            "page": page,
            "where": where_statement
        }

        identities = models.identity.query(query)
        for identity in identities:
            queue.put_details("pull sources", {"identity": identity})

        if len(identities) == per_page:
            task.update({"page": page + 1})
            queue.put_task(task)
    
    return identity_follow_fanout

def set_pull_sources(Client, queue):
    def pull_sources(task):
        identity = task.details.get("identity")
        if identity == None:
            raise Exception("pull posts task requires an identity to run")
        
        base_url = identity["base_url"]
        mastodon_client = models.mastodon_client.find({"base_url": base_url})
    
        if mastodon_client == None:
            client = Client(identity)
        else:
            client = Client(mastodon_client, identity)

        data = client.list_sources()
        _sources = client.map_sources(data)

        sources = []
        for _source in _sources:
            source = models.source.upsert(_source)
            sources.append(source)
      
        reconcile_sources(identity["person_id"], sources)

        for source in sources:
            queue.put_details("pull posts", {
                "client": client,
                "source": source,
            })

    return pull_sources

def set_pull_posts(queue):
    def pull_posts(task):
        client = task.details.get("client")
        source = task.details.get("source")
        if client == None or source == None:
            raise Exception("pull posts task lacks needed inputs")

        link = models.source.get_last_retrieved(source["id"])
        source["last_retrieved"] = link.get("secondary")


        last_retrieved = joy.time.now()
        data = client.get_post_graph(models, source)
        link["secondary"] = last_retrieved
        models.link.update(link["id"], link)


        sources = []
        _sources = client.map_sources(data)
        for _source in _sources:
            source = models.source.upsert(_source)
            sources.append(source)
        data["sources"] = sources


        # Get GOBO IDs for the posts and link to their author sources.
        post_data = client.map_posts(data)
        posts = []
        id_map = {}
        for _post in post_data["posts"]:
            post = models.post.upsert(_post)
            posts.append(post)
            id_map[post["profile_id"]] = post["id"]

            models.link.upsert({
                "origin_type": "source",
                "origin_id": post["source_id"],
                "target_type": "post",
                "target_id": post["id"],
                "name": "has-post",
                "secondary": f"{post['published']}::{post['id']}"
            })


        # Establish inter-post edges using the confirmed GOBO IDs.
        for edge in post_data["edges"]:
            if edge["origin_type"] == "post" and edge["target_type"] == "post":
                models.link.upsert({
                    "origin_type": "post",
                    "origin_id": id_map[edge["origin_reference"]],
                    "target_type": "post",
                    "target_id": id_map[edge["target_reference"]],
                    "name": edge["name"],
                    "secondary": edge.get("secondary")
                })            


        # Add each post to the feeds of each author follower.
        for post in posts:
            queues.database.put_details("add post to followers", {
                "page": 1,
                "per_page": 500,
                "post": post
            })

    return pull_posts

def reconcile_sources(person_id, sources):
    base_url = sources[0]["base_url"]
    desired_sources = []
    for source in sources:
        desired_sources.append(source["id"])
    
    results = models.link.pull([
        where("origin_type", "person"),
        where("origin_id", person_id),
        where("target_type", "source"),
        where("name", "follows")
    ])
   
    current_sources = []
    source_ids = [ result["target_id"] for result in results ]
    for source in models.source.pluck(source_ids):
        if source["base_url"] == base_url:
            current_sources.append(source["id"])


    difference = list(set(desired_sources) - set(current_sources))
    for source_id in difference:
        logging.info(f"For person {person_id}, adding source {source_id}")
        queues.database.put_details("follow", {
            "person_id": person_id,
            "source_id": source_id
        })

    difference = list(set(current_sources) - set(desired_sources))
    for source_id in difference:
        logging.info(f"For person {person_id}, removing source {source_id}")
        queues.database.put_details("unfollow", {
            "person_id": person_id,
            "source_id": source_id
        })