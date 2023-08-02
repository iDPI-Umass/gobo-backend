import logging
import json
import re
from atproto import Client
from atproto.xrpc_client import models
import joy
from .helpers import guess_mime


def isRepost(data):
    reason = data.get("reason", None)

    if reason is None:
        return False
    if reason["_type"] != "app.bsky.feed.defs#reasonRepost":
        return False
    return True

def isReply(data):
    reply = data.get("reply", None)

    if reply is None:
        return False
    if reply["_type"] != "app.bsky.feed.defs#replyRef":
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


def build_post(item):
    data = parse_object(item)
    try:
        # logging.info(json.dumps(data, indent = 2, default = json_failure))
        return Post.create(data)
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(json.dumps(data, indent = 2, default = json_failure))
        logging.error("\n\n")
        return None


def get_regular_content(data):
    text = data["post"]["record"].get("text", "")
    if isReply(data):
        actor = data["reply"]["parent"]["author"]
        text = f"{Actor.get_anchor(actor)} {text}"
    return text


def get_attachments(post):
    attachments = []
    
    imbed = post.get("embed", None)
    if imbed is None:
        return attachments

    media = imbed.get("media", None)
    if media is not None:
        images = media.get("images", None)
    else:
        images = imbed.get("images", None)
    
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


def get_regular_share(post):
    # logging.info(json.dumps(post, indent = 2, default = json_failure))
    embed = post.get("embed")
    if embed is None:
        return None

    record = embed.get("record")
    if record is None:
        return None

    # TODO: wtf bluesky?
    if record.get("record", None) is not None:
        record = record["record"]
    
    author = record.get("author")
    if author is None:
        return None

    # There's a split in indentification.
    # The Bluesky URL uses the random identifier with the author's handle.
    # The full ID uses the random identifier with the author's did
    value = record["value"]
    match = post_id_regex.search(record["uri"])
    id = match.group(1)
    
    self = Post()
    self.id = record["uri"]
    self.author = Actor(author)
    self.content = value.get("text", "")
    self.url = f"{Bluesky.BASE_URL}/profile/{self.author.username}/post/{id}"
    self.published = value.get("createdAt", record.get("created_at", None))
      
    self.attachments = get_attachments(record)
    self.share = None # get_regular_share(record)
    self.poll = None

    return self



class Post():
    @staticmethod
    def create(data):
        if isRepost(data):
            return Post.create_repost(data)
        else:
            return Post.create_regular(data)


    @staticmethod
    def create_repost(data):
        post = data["post"]
        reason = data["reason"]

        self = Post()
        self.share = Post.create_regular(data)
        self.author = Actor(reason["by"])
        self.content = None
        self.url = self.share.url
        self.attachments = []
        self.poll = None
        
        self.published = reason.get("indexedAt", reason.get("indexed_at", None))

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

    @staticmethod
    def create_regular(data):
        post = data["post"]
        record = post["record"]

        # There's a split in indentification.
        # The Bluesky URL uses the random identifier with the author's handle.
        # The full ID uses the random identifier with the author's did
        match = post_id_regex.search(post["uri"])
        id = match.group(1)
        
        self = Post()
        self.id = post["uri"]
        self.author = Actor(post["author"])
        self.content = get_regular_content(data)
        self.url = f"{Bluesky.BASE_URL}/profile/{self.author.username}/post/{id}"
        self.published = record.get("createdAt", record.get("created_at", None))
          
        self.attachments = get_attachments(post)
        self.share = get_regular_share(post)
        self.poll = None

        return self



class Actor():
    def __init__(self, data):
        did = data["did"]
        handle = data["handle"]
        if handle.endswith(r".bsky.social"):
            url = f"{Bluesky.BASE_URL}/profile/{handle}"
        else:
            url = f"{Bluesky.BASE_URL}/profile/{did}"

        self.id = did
        self.url = url
        self.username = handle
        self.name = data.get("displayName", data.get("display_name", None))
        self.icon_url = data["avatar"]

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

    def __init__(self, identity):
        self.identity = identity
        self.me = self.identity["oauth_token"]
        self.client = Client()
        self.client.login(
            login = identity.get("oauth_token", None),
            password = identity.get("oauth_token_secret", None)
        )

    def get_profile(self):
        return self.client.bsky.actor.get_profile({
            "actor": self.me
        })




    def list_sources(self):
        id = self.identity["platform_id"]

        actors = []
        actors.append(Actor(self.get_profile()))
        cursor = None
        while True:
            result = self.client.bsky.graph.get_follows({
                "actor": self.me,
                "cursor": cursor
            })

            for item in result.follows:
                actors.append(Actor(item))

            cursor = result.cursor
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

            result = self.client.bsky.feed.get_author_feed({
                "actor": source["username"],
                "cursor": cursor
            })

            if last_retrieved is None:
                for item in result.feed:
                    post = build_post(item)
                    if post is None:
                        continue

                    count += 1
                    if count < 50:
                        posts.append(post)
                    else:
                        isDone = True
                        break
            else:
                for item in result.feed:
                    post = build_post(item)
                    if post is None:
                        continue

                    if post.published > last_retrieved:
                        posts.append(post)
                    else:
                        isDone = True
                        break
          
            cursor = result.cursor
            if cursor is None:
                break
         


        seen_posts = set()
        for post in posts:
            seen_posts.add(post.id)
        for post in posts:
            share = post.share
            if share is not None and share.id not in seen_posts:
                seen_posts.add(share.id)
                partials.append(share)
                share = share.share
                if share is not None and share is type(Post) and share.id not in seen_posts:
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
