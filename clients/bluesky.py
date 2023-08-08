import logging
import json
import re
from .gobo_bluesky import GOBOBluesky
import joy
from .helpers import guess_mime


def isRepost(data):
    reason = data.get("reason", None)

    if reason is None:
        return False
    if reason["$type"] != "app.bsky.feed.defs#reasonRepost":
        return False
    return True

def json_failure(value):
    logging.info(vars(value))
    return "Unable to stringify this value"

post_id_regex = re.compile(r"app\.bsky\.feed\.post/(.+)$")

primitive = (str, int, float, bool, type(None), bytes, bytearray)

def parse_object(input):
    if isinstance(input, primitive):
        return input
    elif isinstance(input, list):
        output = []
        for item in input:
            output.append(parse_object(item))
        return output
    elif isinstance(input, dict):
        output = {}
        for key, value in input.items():
            output[key] = parse_object(value)
        return output
    else:
        return parse_object(vars(input))


def build_post(data):
    try:
        # logging.info(json.dumps(data, indent = 2, default = json_failure))
        return Post.create(data)
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(json.dumps(data, indent = 2, default = json_failure))
        logging.error("\n\n")
        return None


def get_attachments(embed):
    attachments = []
    
    images = embed.get("images", None)
    if images is None:
        return attachments

    # TODO: This is hacky, but I'm worried about the stablility of finding the
    #       correct record stanza with the embed that contains an explicit mime type.
    for image in images:
        url = image["fullsize"]
        attachments.append({
          "url": url,
          "type": guess_mime(url.replace("@", "."))
        })

    return attachments


def get_record_view(data):
    record = data.get("record")
    author = record.get("author")
    value = record.get("value")

    # TODO: This isn't a post. It looks like a list of sources, which we might
    #       want to represent with GOBO abstractions. Punt for now.
    if record["$type"] == "app.bsky.feed.defs#generatorView":
        return None
    
    # TODO: How do we want to represent graph structures where one of the nodes
    #       is defunct? It's mostly a client-side issue, so I wonder if this
    #       should be some sort of constant, like Bluesky's approach.
    if record["$type"] == "app.bsky.embed.record#viewNotFound":
        return None
    
    # TODO: Our assumption for a follower being able to view this source's post
    #       does not apply to secondary and tetiary posts in the graph.
    #       Punt for now, which errs on the side of privacy, but there's an
    #       an access control calculation problem here.
    if record["$type"] == "app.bsky.embed.record#viewBlocked":
        return None

    self = Post()
    self.id = record["uri"]
    self.author = Actor(author)
    self.content = value.get("text", "")
    self.url = Post.get_url(self)
    self.published = value.get("createdAt", None)

    self.attachments = []
    self.share = None
    self.reply = None
    self.poll = None

    embeds = record.get("embeds", [])
    for embed in embeds:
        if embed["$type"] in ["app.bsky.embed.record#view", "app.bsky.embed.record#viewRecord"]:
            self.share = get_record_view(embed)
        elif embed["$type"] == "app.bsky.embed.recordWithMedia#view":
            self.share = get_record_view(embed["record"])
            self.attachments = get_attachments(embed["media"])
        elif embed["$type"] == "app.bsky.embed.images#view":
            self.attachments = get_attachments(embed)

    return self


def get_reply(data):
    reply = data.get("reply")
    if reply is None:
        return None
    
    post = reply.get("parent")
    if post is None:
        return None

    return Post.create_core(post)


class Post():
    @staticmethod
    def create(data):
        if data is None:
            raise Exception("raw dictionary passed to Post constructor is None")

        if isRepost(data):
            return Post.create_repost(data)
        else:
            return Post.create_regular(data)


    @staticmethod
    def get_url(post):
        # There's a split in indentification.
        # The Bluesky URL uses the random identifier with the author's handle.
        # The full ID uses the random identifier with the author's did
        match = post_id_regex.search(post.id)
        id = match.group(1)
        return f"{Bluesky.BASE_URL}/profile/{post.author.username}/post/{id}"


    @staticmethod
    def create_core(post):
        author = post["author"]
        record = post["record"]

        self = Post()
        self.id = post["uri"]
        self.author = Actor(author)
        self.content = record.get("text", None)
        self.url = Post.get_url(self)
        self.published = record.get("createdAt", None)
          
        self.attachments = []
        self.is_repost = False
        self.share = None
        self.reply = None
        self.poll = None

        embed = post.get("embed", {"$type": None})
        if embed["$type"] in ["app.bsky.embed.record#view", "app.bsky.embed.record#viewRecord"]:
            self.share = get_record_view(embed)
        elif embed["$type"] == "app.bsky.embed.recordWithMedia#view":
            self.share = get_record_view(embed["record"])
            self.attachments = get_attachments(embed["media"])
        elif embed["$type"] == "app.bsky.embed.images#view":
            self.attachments = get_attachments(embed)
        elif embed["$type"] == "app.bsky.embed.external#view":
            url = embed["external"].get("uri", "")
            self.content += f"\n\n {url}"

        return self
    

    @staticmethod
    def create_regular(data):
        self = Post.create_core(data["post"])
        self.reply = get_reply(data)
        return self
    

    @staticmethod
    def create_repost(data):
        post = data["post"]
        reason = data["reason"]

        self = Post()
        self.is_repost = True
        self.share = Post.create_regular(data)
        self.author = Actor(reason["by"])
        self.content = None
        self.url = self.share.url
        self.published = reason.get("indexedAt", None)

        self.attachments = []
        self.reply = None
        self.poll = None

        # Bluesky (or this client library) represents reposts as virtual resources
        # without a standalone ID or URI. GOBO's abstract post model at a minimum
        # benefits from such an ID. In my opinion, design pressures from our
        # graph model point to a standalone resource as the right approach to
        # offer optionality in the future. I'm going to create a virtual
        # ID as a placeholder for this resource within GOBO.

        # TODO: This suggests that we should model the relationship between
        # posts with empty content and their targets as "reposts" instead of
        # uniformly using "shares". The latter works for our immediate needs,
        # but graph calculations could be helped by that hint.

        self.id = f"gobo:{self.author.id}:{self.published}:{post['cid']}"
        return self



class Actor():
    def __init__(self, data):
        if data is None:
            raise Exception("raw dictionary passed to Actor constructor is None")

        did = data["did"]
        handle = data["handle"]

        self.id = did
        self.url = Actor.get_url(data)
        self.username = handle
        self.name = data.get("displayName", None)
        self.icon_url = data.get("avatar", None)

    @staticmethod
    def get_url(data):
        did = data["did"]
        handle = data["handle"]
        if handle.endswith(r".bsky.social"):
            return f"{Bluesky.BASE_URL}/profile/{handle}"
        else:
            return f"{Bluesky.BASE_URL}/profile/{did}"

    @staticmethod
    def get_anchor(data):
        handle = data["handle"]
        url = Actor.get_url(data)
        return f'<a href={url} target="_blank" rel="noopener noreferrer nofollow">@{handle}</a>'


class Bluesky():
    BASE_URL = "https://bsky.app"

    def __init__(self, identity = None):
        self.seen_types = set()
        self.seen_type_examples = {}

        if identity is not None:
            self.identity = identity
            self.me = self.identity["oauth_token"]
            self.client = GOBOBluesky()
            self.client.login(
                login = identity.get("oauth_token", None),
                password = identity.get("oauth_token_secret", None)
            )

    def get_profile(self):
        return self.client.get_profile(self.me)
    
    def parse_types(self, input):
        if isinstance(input, primitive):
            return
        elif isinstance(input, list):
            for item in input:
                self.parse_types(item)
            return
        elif isinstance(input, dict):
            _type = input.get("$type", None)
            if _type is not None:
                self.seen_types.add(_type)
                if self.seen_type_examples.get(_type, None) is None:
                    self.seen_type_examples[_type] = []
                self.seen_type_examples[_type].append(input)

            for key, value in input.items():
                self.parse_types(value)
            return
        else:
            raise Exception("this is supposed to be JSON")
        
    @staticmethod
    def build_post(post):
        return build_post(post)


    def list_sources(self):
        id = self.identity["platform_id"]

        actors = []
        actors.append(Actor(self.get_profile()))
        cursor = None
        while True:
            result = self.client.get_follows(self.me, cursor)
            for item in result["feed"]:
                actors.append(Actor(item))

            cursor = result.get("cursor", None)
            if cursor is None:
                break

        return {"actors": actors}


    def map_sources(self, data):
        sources = []
        for actor in data["actors"]:
            sources.append({
                "platform_id": actor.id,
                "base_url": self.BASE_URL,
                "url": actor.url,
                "username": actor.username,
                "name": actor.name,
                "icon_url": actor.icon_url,
                "active": True
            })
  
        return sources


    def map_posts(self, data):        
        sources = {}
        for item in data["sources"]:
            sources[item["platform_id"]] = item
        
        
        posts = []
        partials = []
        edges = []
        for post in data["posts"]:
            if post.id is None:
                continue

            source = sources[post.author.id]

            posts.append({
                "source_id": source["id"],
                "base_url": Bluesky.BASE_URL,
                "platform_id": post.id,
                "title": None,
                "content": post.content,
                "url": post.url,
                "published": post.published,
                "attachments": post.attachments,
                "poll": post.poll
            })

            if post.share != None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": post.id,
                    "target_type": "post",
                    "target_reference": post.share.id,
                    "name": "shares",
                })

            if post.reply != None:
                if post.is_repost == True:
                    edges.append({
                        "origin_type": "post",
                        "origin_reference": post.share.id,
                        "target_type": "post",
                        "target_reference": post.reply.id,
                        "name": "replies",
                    })
                else:
                    edges.append({
                        "origin_type": "post",
                        "origin_reference": post.id,
                        "target_type": "post",
                        "target_reference": post.reply.id,
                        "name": "replies",
                    })

        for post in data["partials"]:
            if post.id is None:
                continue

            source = sources[post.author.id]

            partials.append({
                "source_id": source["id"],
                "base_url": Bluesky.BASE_URL,
                "platform_id": post.id,
                "title": None,
                "content": post.content,
                "url": post.url,
                "published": post.published,
                "attachments": post.attachments,
                "poll": post.poll
            })

            if post.share != None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": post.id,
                    "target_type": "post",
                    "target_reference": post.share.id,
                    "name": "shares",
                })

            if post.reply != None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": post.id,
                    "target_type": "post",
                    "target_reference": post.reply.id,
                    "name": "replies",
                })

        return {
            "posts": posts,
            "partials": partials,
            "edges": edges
        }



    def get_post_graph(self, source):
        posts = []
        partials = []
        actors = []
        cursor = None
        last_retrieved = source.get("last_retrieved", None)
        isDone = False
        count = 1
        while True:
            if isDone == True:
                break

            result = self.client.get_author_feed(source["username"], cursor)

            if last_retrieved is None:
                for item in result["feed"]:
                    post = build_post(item)
                    if post is None:
                        continue

                    count += 1
                    if count < 4:
                        posts.append(post)
                    else:
                        isDone = True
                        break
            else:
                for item in result["feed"]:
                    post = build_post(item)
                    if post is None:
                        continue

                    if post.published > last_retrieved:
                        posts.append(post)
                    else:
                        isDone = True
                        break
          
            cursor = result.get("cursor", None)
            if cursor is None:
                break
         


        seen_posts = set()
        for post in posts:
            seen_posts.add(post.id)
        for post in posts:
            reply = post.reply
            if reply is not None and reply.id not in seen_posts:
                seen_posts.add(reply.id)
                partials.append(reply)

            share = post.share
            if share is not None and share.id not in seen_posts:
                seen_posts.add(share.id)
                partials.append(share)
                
                reply = share.reply
                if reply is not None and reply.id not in seen_posts:
                    seen_posts.add(reply.id)
                    partials.append(reply)

                share = share.share
                if share is not None and share.id not in seen_posts:
                    seen_posts.add(share.id)
                    partials.append(share)
      


        seen_actors = set()
        for post in posts:
            actor = post.author
            if actor.id not in seen_actors:
                seen_actors.add(actor.id)
                actors.append(actor)
        for post in partials:
            actor = post.author
            if actor.id not in seen_actors:
                seen_actors.add(actor.id)
                actors.append(actor)


        return {
            "posts": posts,
            "partials": partials,
            "actors": actors
        }
