import logging
import re
from atproto import Client
from atproto.xrpc_client import models
import joy


def isRepost(item):
  if item.reason is None:
      return False
  if item.reason._type != "app.bsky.feed.defs#reasonRepost":
      return False
  return True

post_id_regex = re.compile(r"app\.bsky\.feed\.post/(.+)$")

class Post():
    @staticmethod
    def create(_):
        if isRepost(_):
            return Post.repost_create(_)
        else:
            return Post.regular_create(_)

    @staticmethod
    def regular_create(_):
        self = Post()

        # There's a split in indentification.
        # The Bluesky URL uses the random identifier with the author's handle.
        # The full ID uses the random identifier with the author's did 
        match = post_id_regex.search(_.post.uri)
        id = match.group(1)
        
        self.id = _.post.uri
        self.author = Actor(_.post.author)
        self.content = _.post.record.text
        self.url = f"{Bluesky.BASE_URL}/profile/{self.author.username}/post/{id}"
        self.published = getattr(
            _.post.record, 
            "createdAt", 
            getattr(_.post.record, "created_at", None)
        )

      
        self.attachments = []
        if hasattr(_.post, "embed") and hasattr(_.post.embed, "images") and _.post.embed.images is not None:
            types = {}
            for item in _.post.record.embed.images:
                types[item.image.ref] = item.image.mime_type

            for item in _.post.embed.images:
                url = item.fullsize
                match = "image/jpeg"
                for key, value in types.items():
                    if key in url:
                        match = value
                        break

                self.attachments.append({
                    "url": url,
                    "type": match
                })

        self.share = None
        if hasattr(_.post, "embed") and hasattr(_.post.embed, "author") and _.post.embed.author is not None:
            self.share = Post.regular_create(_.post.embed)

        self.poll = None

        return self
        

    @staticmethod
    def repost_create(_):
        self = Post()
        self.share = Post.regular_create(_)

        # Bluesky (or this client library) represents reposts as virtual resources
        # without a standalone ID or URI. GOBO's abstract post model at a minimum
        # benefits from such an ID. In my opinion, design pressures from our
        # graph model point to a standalone resource as the right approach to
        # offer optionality in the future. I'm going to create a virtual
        # ID as a placeholder for this resource within GOBO.
        self.id = f"gobo:{joy.crypto.random({'encoding': 'safe-base64'})}"

        # TODO: This suggests that we should model the relationship between
        # posts with empty content and their targets as "reposts" instead of
        # uniformly using "shares". The latter works for our immediate needs,
        # but graph calculations could be helped by that hint.


        self.author = Actor(_.reason.by)
        self.content = None
        self.url = self.share.url
        self.published = getattr(
            _.reason, 
            "indexedAt", 
            getattr(_.reason, "indexed_at", None)
        )

        self.attachments = []
        self.poll = None

        return self


      
          



class Actor():
    def __init__(self, _):
        self.id = _.did
        self.url = f"{Bluesky.BASE_URL}/profile/{_.handle}"
        self.username = _.handle
        self.name = getattr(
            _, 
            "displayName", 
            getattr(_, "display_name", None)
        ),
        self.icon_url = _.avatar



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
        cursor = None
        while True:
            logging.info("pulling follows")
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
                    count += 1
                    if count < 1000:
                        posts.append(Post.create(item))
                    else:
                        isDone = True
                        break
            else:
                for item in result.feed:
                    post = Post.create(item)
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
