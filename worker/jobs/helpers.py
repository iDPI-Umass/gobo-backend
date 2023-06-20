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

        client = Client(identity)
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
        data = client.list_posts(source)
        link["secondary"] = last_retrieved
        models.link.update(link["id"], link)

        sources = []
        _sources = client.map_sources(data)
        for _source in _sources:
            source = models.source.upsert(_source)
            sources.append(source)
        data["sources"] = sources

        _posts = client.map_posts(source, data)
        posts = []
        for _post in _posts:
            post = models.post.upsert(_post)
            posts.append(post)
            models.link.upsert({
                "origin_type": "source",
                "origin_id": source["id"],
                "target_type": "post",
                "target_id": post["id"],
                "name": "has-post",
                "secondary": post["published"]
            })

        for post in posts:
            queues.database.put_details("add post to followers", {
                "page": 1,
                "per_page": 500,
                "post": post
            })

    return pull_posts

def reconcile_sources(person_id, sources):
    desired_sources = []
    for source in sources:
        desired_sources.append(source["id"])
    
    results = models.link.pull({
        "where": [
            where("origin_type", "person"),
            where("origin_id", person_id),
            where("target_type", "source"),
            where("name", "follows")
        ]
    })
   
    current_sources = []
    for result in results:
        current_sources.append(result["target_id"])


    difference = list(set(desired_sources) - set(current_sources))
    for source_id in difference:
        queues.database.put_details("follow", {
            "person_id": person_id,
            "source_id": source_id
        })

    difference = list(set(current_sources) - set(desired_sources))
    for source_id in difference:
        queues.database.put_details("unfollow", {
            "person_id": person_id,
            "source_id": source_id
        })