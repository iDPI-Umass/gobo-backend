import logging
from os import environ
from datetime import timedelta
import json
from jose import jwt
import re
import joy
import models
from .gobo_bluesky import GOBOBluesky
from .helpers import guess_mime


# NOTE: These are taken from the atproto documentation: https://atproto.com/blog/create-post
def parse_mentions(text):
    spans = []
    # regex based on: https://atproto.com/specs/handle#handle-identifier-syntax
    mention_regex = rb"[$|\W](@([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(mention_regex, text_bytes):
        spans.append({
            "start": m.start(1),
            "end": m.end(1),
            "handle": m.group(1)[1:].decode("UTF-8")
        })
    return spans

def parse_links(text):
    spans = []
    # partial/naive URL regex based on: https://stackoverflow.com/a/3809435
    # tweaked to disallow some training punctuation
    url_regex = rb"[$|\W](https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*[-a-zA-Z0-9@%_\+~#//=])?)"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(url_regex, text_bytes):
        spans.append({
            "start": m.start(1),
            "end": m.end(1),
            "uri": m.group(1).decode("UTF-8"),
        })
    return spans





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

post_rkey_regex = re.compile(r"app\.bsky\.feed\.post/(.+)$")
like_rkey_regex = re.compile(r"app\.bsky\.feed\.like/(.+)$")
repost_rkey_regex = re.compile(r"app\.bsky\.feed\.repost/(.+)$")

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


def get_external(embed):
    thumb = embed["external"].get("thumb", None)
    
    return {
        "type": "application/json+gobo-syndication",
        "source": embed["external"]["uri"],
        "title": embed["external"]["title"],
        "description": embed["external"]["description"],
        "media": thumb
    }


def sort_facets(facet):
    return facet["index"]["byteStart"]


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
    self.id = json.dumps({"uri": record["uri"], "cid": record["cid"]})
    self.author = Actor(author)
    self.content = value.get("text", "")
    self.url = Post.get_url(self)
    self.published = value.get("createdAt", None)
    self.facets = value.get("facets", None)

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
            self.attachments.extend(get_attachments(embed["media"]))
        elif embed["$type"] == "app.bsky.embed.images#view":
            self.attachments.extend(get_attachments(embed))
        elif embed["$type"] == "app.bsky.embed.external#view":
            self.attachments.append(get_external(embed))

    self.apply_facets()
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

    # Identification in Bluesky is convoluted. Posts have an "rkey" value
    # but referencing a post requires uri (author's did + rkey) and the cid (content hash)
    # and the URL uses the author's handle + rkey
    @staticmethod
    def get_rkey(uri):
        match = post_rkey_regex.search(uri)
        return match.group(1)

    @staticmethod
    def get_url(post):
        rkey = Post.get_rkey(json.loads(post.id)["uri"])
        return f"{Bluesky.BASE_URL}/profile/{post.author.username}/post/{rkey}"


    @staticmethod
    def create_core(post):
        author = post["author"]
        record = post["record"]

        self = Post()
        self.id = json.dumps({"uri": post["uri"], "cid": post["cid"]})
        self.author = Actor(author)
        self.content = record.get("text", None)
        self.url = Post.get_url(self)
        self.published = record.get("createdAt", None)
        self.facets = record.get("facets", None)
          
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
            self.attachments.extend(get_attachments(embed["media"]))
        elif embed["$type"] == "app.bsky.embed.images#view":
            self.attachments.extend(get_attachments(embed))
        elif embed["$type"] == "app.bsky.embed.external#view":
            self.attachments.append(get_external(embed))

        self.apply_facets()
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
    


    # facets are Bluesky's approach to expressing hypertext linkages in post
    # content. This applies facet data we pull from each post graph and renders
    # it as something more Web-friendly for GOBO.

    # NOTE: Be careful to use binary strings so the byte index values are accurate.
    def apply_facets(self):
        if self.facets is None:
            return
        
        self.facets.sort(key = sort_facets)
        
        original = self.content.encode()
        text = ""
        offset = 0
        for facet in self.facets:
            start = facet["index"]["byteStart"] - offset
            end = facet["index"]["byteEnd"] - offset
          
            if facet["features"][0]["$type"] == "app.bsky.richtext.facet#mention":
                did = facet["features"][0]["did"]
                text += original[:start].decode()
                target = original[start:end].decode()
                text += f"<a rel='nofollow noopener noreferrer' target='_blank' href='https://bsky.app/profile/{did}'>{target}</a>"
                original = original[end:]
                offset = end

            if facet["features"][0]["$type"] == "app.bsky.richtext.facet#link":
                uri = facet["features"][0]["uri"]
                text += original[:start].decode()
                target = original[start:end].decode()
                text += f"<a rel='nofollow noopener noreferrer' target='_blank' href='{uri}'>{target}</a>"
                original = original[end:]
                offset = end

        text += original.decode()
        self.content = text



class Like():
    @staticmethod
    def get_rkey(uri):
        match = like_rkey_regex.search(uri)
        return match.group(1)
    
class Repost():
    @staticmethod
    def get_rkey(uri):
        match = repost_rkey_regex.search(uri)
        return match.group(1)


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


class Session():
    @staticmethod
    def create(identity, session):
        if identity is None:
            raise Exception("raw dictionary passed to Session constructor is None")
        if session is None:
            raise Exception("raw dictionary passed to Session constructor is None")

        access_token = session["accessJwt"]
        expires = jwt.get_unverified_claims(access_token)["exp"]
        access_expires = joy.time.convert("unix", "iso", expires)     
        refresh_token = session["refreshJwt"]
        expires = jwt.get_unverified_claims(refresh_token)["exp"]
        refresh_expires = joy.time.convert("unix", "iso", expires)
        return {
            "person_id": identity["person_id"],
            "base_url": Bluesky.BASE_URL,
            "handle": session["handle"],
            "did": session["did"],
            "access_token": access_token,
            "access_expires": access_expires,
            "refresh_token": refresh_token,
            "refresh_expires": refresh_expires
        }


class Bluesky():
    BASE_URL = "https://bsky.app"

    def __init__(self, identity):
        self.identity = identity
        self.me = self.identity["oauth_token"]
        self.client = GOBOBluesky()


    @staticmethod
    def create_session(identity):
        client = GOBOBluesky()
        return client.login(
            login = identity.get("oauth_token", None),
            password = identity.get("oauth_token_secret", None)
        )
    
    @staticmethod
    def refresh_session(session):
        client = GOBOBluesky()
        return client.refresh_session(session)
    
    @staticmethod
    def map_session(identity, session):
        return Session.create(identity, session)
    
    
    def login(self):
        session = models.bluesky_session.find({
            "person_id": self.identity["person_id"],
            "base_url": self.identity["base_url"],
            "did": self.identity["platform_id"]
        })
        if session is None:
            raise Exception("bluesky client: no matching session for this identity")

        self.client.load_session(session)


    def get_profile(self):
        return self.client.get_profile(self.me)
    
    
    # facets are Bluesky's approach to expressing hypertext linkages in post
    # content. We need to parse the post content and format data for Bluesky.
    def parse_facets(self, data):
        text = data["text"]
        facets = []

        for match in parse_mentions(text):
            logging.info(match)
            did = self.client.resolve_handle(match["handle"])["did"]
            facets.append({
                "index": {
                    "byteStart": match["start"],
                    "byteEnd": match["end"],
                },
                "features": [{
                    "$type": "app.bsky.richtext.facet#mention", 
                    "did": did
                }],
            })

        for match in parse_links(text):
            logging.info(match)
            facets.append({
                "index": {
                    "byteStart": match["start"],
                    "byteEnd": match["end"],
                },
                "features": [{
                    "$type": "app.bsky.richtext.facet#link", 
                    "uri": match["uri"]
                }],
            })

        data["facets"] = facets 


    def create_post(self, post, metadata):
        embed = {
            "images": None,
            "record": None
        }
        
        images = []
        for attachment in post.get("attachments", []):
            result = self.client.upload_blob(attachment)
            images.append({
                "image": result["blob"],
                "alt": attachment["alt"]
            })
        if len(images) > 0:
            embed["images"] = images

        if metadata.get("quote", None) is not None:
            embed["record"] = json.loads(metadata["quote"]["platform_id"])

        has_images = embed["images"] is not None
        has_record = embed["record"] is not None
        if has_record and has_images:
            embed["$type"] = "app.bsky.embed.recordWithMedia"
        elif has_record:
            embed["$type"] = "app.bsky.embed.record"
            del embed["images"]
        elif has_images:
            embed["$type"] = "app.bsky.embed.images"
            del embed["record"]
        else:
            embed = None
       

        reply = None
        if metadata.get("reply", None) is not None:
            uri = json.loads(metadata["reply"]["platform_id"])["uri"]
            parent = self.client.get_post(Post.get_rkey(uri))
            grandparent = parent.get("reply", None)
            reply = {
                "parent": {
                    "uri": parent["uri"],
                    "cid": parent["cid"]
                }
            }
            if grandparent is None:
                reply["root"] = reply["parent"]
            else:
                reply["root"] = grandparent["root"]


        post_data = {
            "text": post.get("content", ""),
            "createdAt": joy.time.now()
        }

        if embed is not None:
            post_data["embed"] = embed
        if reply is not None:
            post_data["reply"] = reply

        self.parse_facets(post_data)
        # logging.info(post_data)
        return self.client.create_post(post_data)



    def like_post(self, post):
        return self.client.like_post({
            "subject": json.loads(post["platform_id"]),
            "createdAt": joy.time.now(),
            "$type": "app.bsky.feed.like"
        })
    
    def undo_like_post(self, edge):
        rkey = Like.get_rkey(edge["stash"]["uri"])
        return self.client.undo_like_post(rkey)
    
    def repost_post(self, post):
        return self.client.repost_post({
            "subject": json.loads(post["platform_id"]),
            "createdAt": joy.time.now(),
            "$type": "app.bsky.feed.repost"
        })
    
    def undo_repost_post(self, edge):
        rkey = Repost.get_rkey(edge["stash"]["uri"])
        return self.client.undo_repost_post(rkey)


    def list_sources(self):
        id = self.identity["platform_id"]

        actors = []
        actors.append(Actor(self.get_profile()))
        cursor = None
        while True:
            result = self.client.get_follows(self.me, cursor)
            for item in result["follows"]:
                actors.append(Actor(item))

            cursor = result.get("cursor", None)
            if cursor is None:
                break

        return {"actors": actors}


    def map_sources(self, data):
        sources = []
        for actor in data["actors"]:
            sources.append({
                "platform": "bluesky",
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

        def map_post(source, post):
            return {
                "source_id": source["id"],
                "base_url": Bluesky.BASE_URL,
                "platform": "bluesky",
                "platform_id": post.id,
                "title": None,
                "content": post.content,
                "url": post.url,
                "published": post.published,
                "attachments": post.attachments,
                "poll": post.poll
            }


        for post in data["posts"]:
            if post.id is None:
                continue
            source = sources[post.author.id]
            posts.append(map_post(source, post))


        for post in data["partials"]:
            if post.id is None:
                continue
            source = sources[post.author.id]
            partials.append(map_post(source, post))

           
        for post in (data["posts"] + data["partials"]):
            if post.id is None:
                continue
            
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



    def get_post_graph(self, source, last_retrieved = None, is_shallow = False):
        posts = []
        partials = []
        actors = []
        cursor = None
        oldest_limit = joy.time.convert("date", "iso", 
            joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
        )
        if is_shallow == True:
            default_limit = 100
        else:
            default_limit = 400
        
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
                    if post.published < oldest_limit:
                        isDone = True
                        break
                    if count < default_limit:
                        posts.append(post)
                    else:
                        isDone = True
                        break
            else:
                for item in result["feed"]:
                    post = build_post(item)
                    if post is None:
                        continue

                    if post.published < oldest_limit:
                        isDone = True
                        break
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
