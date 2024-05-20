import logging
from os import environ
from datetime import datetime, timedelta
import json
from jose import jwt
import re
import joy
import models
from .gobo_bluesky import GOBOBluesky
import clients.helpers as h
from .http_error import HTTPError


# NOTE: These are taken from the atproto documentation: https://atproto.com/blog/create-post
def parse_mentions(text):
    spans = []
    # regex based on: https://atproto.com/specs/handle#handle-identifier-syntax
    mention_regex = rb"(^|\W)(@([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(mention_regex, text_bytes):
        spans.append({
            "start": m.start(2),
            "end": m.end(2),
            "handle": m.group(2)[1:].decode("UTF-8")
        })
    return spans

def parse_links(text):
    spans = []
    # partial/naive URL regex based on: https://stackoverflow.com/a/3809435
    # tweaked to disallow some training punctuation
    url_regex = rb"(^|\W)((https?:\/\/)?(\w+\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*[-a-zA-Z0-9@%_\+~#//=])?)"
    text_bytes = text.encode("UTF-8")
    for m in re.finditer(url_regex, text_bytes):
        spans.append({
            "start": m.start(2),
            "end": m.end(2),
            "uri": m.group(2).decode("UTF-8"),
        })
    return spans





def is_repost(data):
    reason = data.get("reason", None)

    if reason is None:
        return False
    if reason["$type"] != "app.bsky.feed.defs#reasonRepost":
        return False
    return True

def json_failure(value):
    logging.info(vars(value))
    return "Unable to stringify this value"

parse_uri_regex = re.compile(r"at\:\/\/(.*)\/app\.bsky\.feed\.(?:post|like|repost)\/(.+)$")


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
        # This bit of silliness is because we get labeled objects when we pluck
        # them from the feed, but not from the return value of the getPosts
        # "hydration" request. Looks like interface tree rings.
        if data.get("post") is not None:
            _data = data
        else:
            _data = {"post": data}
        # logging.info(json.dumps(data, indent = 2, default = json_failure))
        return Post.create(_data)
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(json.dumps(data, indent = 2, default = json_failure))
        logging.error("\n\n")
        return None
    
def build_thread(data):
    try:
        # logging.info(json.dumps(data, indent = 2, default = json_failure))
        return Post.create_thread(data["thread"])
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(json.dumps(data, indent = 2, default = json_failure))
        logging.error("\n\n")
        return []


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
          "type": h.guess_mime(url.replace("@", "."))
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
    author = record.get("author", record.get("creator"))
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

    # TODO: This one should probably be reclassified as a syndicated content
    #   attachment. Bluesky treats it like an embed type, it's a summary view
    #   of a list including a text description and author. Is there image?
    if record["$type"] == "app.bsky.graph.defs#listView":
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
    self.thread = None

    # # TODO: These records can have reply data. We don't want to spend time
    # # fetching them right now, but if we start to need to be able to move
    # # through the graph more freely in the future, we'll need this.
    # if record.get("reply", None) is not None:
    #     self.reply = Reply.from_parent(record["reply"]["parent"])


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

        if is_repost(data):
            return Post.create_repost(data)
        else:
            return Post.create_regular(data)

    # Identification in Bluesky is convoluted. Posts have an "rkey" value
    # but referencing a post requires uri (author's did + rkey) and the cid (content hash)
    # and the URL uses the author's handle + rkey
    @staticmethod
    def parse_uri(uri):
        match = parse_uri_regex.search(uri)
        did = match.group(1)
        rkey = match.group(2)
        return { 
            "did": did, 
            "rkey": rkey
        }

    @staticmethod
    def get_rkey(uri):
        values = Post.parse_uri(uri)
        return values.get("rkey")
    
    @staticmethod
    def get_did(uri):
        values = Post.parse_uri(uri)
        return values.get("did")

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
        self.uri = post["uri"]
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
        self.thread = None

        if record.get("reply", None) is not None:
            self.reply = Reply.from_parent(record["reply"]["parent"])

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
        self.thread = None

        # Bluesky (or this client library) represents reposts as virtual resources
        # without a standalone ID or URI. GOBO's abstract post model at a minimum
        # benefits from such an ID. In my opinion, design pressures from our
        # graph model point to a standalone resource as the right approach to
        # offer optionality in the future. I'm going to create a virtual
        # ID as a placeholder for this resource within GOBO.

        # TODO: This suggests that we could model the relationship between
        # posts with empty content and their targets as "reposts" instead of
        # uniformly using "shares". I favor the optionality offered by uniformity.

        self.id = f"gobo:{self.author.id}:{self.published}:{post['cid']}"
        return self
    
    # Traverse the parents in this tree until we can't find any.
    @staticmethod
    def create_thread(data):
        thread = []
        current = data.get("parent")
        while True:
            if current is None or current.get("post") is None:
                break
            thread.append(Post.create_regular(current))
            current = current.get("parent")

        return thread    


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
                offset += end

            if facet["features"][0]["$type"] == "app.bsky.richtext.facet#link":
                uri = facet["features"][0]["uri"]
                text += original[:start].decode()
                target = original[start:end].decode()
                text += f"<a rel='nofollow noopener noreferrer' target='_blank' href='{uri}'>{target}</a>"
                original = original[end:]
                offset += end

        text += original.decode()
        self.content = text


    
# Simplified post class to handle reply references for thread construction.
class Reply():
    def __init__(self, data):
        self.id = data["id"]
        self.uri = data["uri"]

    @staticmethod
    def from_parent(parent):
        uri = parent["uri"]
        cid = parent["cid"]
        id = json.dumps({"uri": uri, "cid": cid})
        return Reply({
            "id": id,
            "uri": uri
        })


class Actor():
    def __init__(self, data):
        if data is None:
            raise Exception("raw dictionary passed to Actor constructor is None")

        did = data["did"]
        handle = data["handle"]
        if handle == "handle.invalid":
            handle = did

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



def build_notification(item, is_active):
    try:
        return Notification(item, is_active)
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(item)
        logging.error("\n\n")
        return None

class Notification():
    def __init__(self, _, is_active):
        self.type = self.map_type(_["reason"])
        self.created = _["indexedAt"]
        self.active = is_active
        self.author = Actor(_["author"])
        self.post = self.map_post(_)
        self.id = f"gobo:notification:{self.type}:{self.author.id}:{_['uri']}:{self.created}"


    def map_type(self, reason):
        if reason in ["follow", "like", "repost", "reply", "quote", "mention"]:
            return reason
        logging.warning(f"Bluesky: unable to map notification type {reason}")
        return reason
    
    # This method is about getting a post URI that we must dereference.
    def map_post(self, _):
        if self.type == "follow":
            return None
        if self.type in ["quote", "reply", "mention"]:
            return _["uri"]
        if self.type in ["repost", "like"]:
            return _["reasonSubject"]
        logging.warning(_)
        raise Exception(f"Bluesky: unable to map notification post")



class SessionFrame():
    def __init__(self, identity):
        self.identity = identity
        self.session = models.bluesky_session.find({
            "identity_id": self.identity["id"]
        })

    def full_login(self):
        client = GOBOBluesky()
        return client.login(
            login = self.identity.get("oauth_token", None),
            password = self.identity.get("oauth_token_secret", None)
        )
    
    def refresh(self):
        client = GOBOBluesky()
        return client.refresh_session(self.session)
    
    @staticmethod
    def map(identity, bundle):
        if identity is None:
            raise Exception("raw identity dictionary passed to SessionFrame constructor is None")
        if bundle is None:
            raise Exception("raw bundle dictionary passed to SessionFrame constructor is None")

        access_token = bundle["accessJwt"]
        expires = jwt.get_unverified_claims(access_token)["exp"]
        access_expires = joy.time.convert("unix", "iso", expires)     
        refresh_token = bundle["refreshJwt"]
        expires = jwt.get_unverified_claims(refresh_token)["exp"]
        refresh_expires = joy.time.convert("unix", "iso", expires)
        return {
            "person_id": identity["person_id"],
            "identity_id": identity["id"],
            "base_url": Bluesky.BASE_URL,
            "handle": bundle["handle"],
            "did": bundle["did"],
            "access_token": access_token,
            "access_expires": access_expires,
            "refresh_token": refresh_token,
            "refresh_expires": refresh_expires
        }

    def refresh_expired(self):
        if self.session is None:
            return True
        timestamp = self.session.get("refresh_expires")
        if timestamp is None:
            return True
        return timestamp < joy.time.now()

    def access_expired(self):
        if self.session is None:
            return True
        timestamp = self.session.get("access_expires")
        if timestamp is None:
            return True
        # Make sure there's at least 10 minutes of access left.
        expires = datetime.fromisoformat(timestamp)
        delta = expires - joy.time.nowdate()
        return delta < timedelta(minutes = 10)

    def cycle_refresh_token(self):
        bundle = self.full_login()
        _session = SessionFrame.map(self.identity, bundle)
        return models.bluesky_session.upsert(_session)

    def cycle_access_token(self):
        bundle = self.refresh()
        _session = SessionFrame.map(self.identity, bundle)
        return models.bluesky_session.upsert(_session)
        
    

class Bluesky():
    BASE_URL = "https://bsky.app"

    def __init__(self, identity):
        self.identity = identity
        self.me = self.identity["oauth_token"]
        self.client = GOBOBluesky()
        self.invalid = False
    
    def login(self):
        session = None
        frame = SessionFrame(self.identity)
        
        if frame.refresh_expired():
            session = frame.cycle_refresh_token()
        elif frame.access_expired():
            session = frame.cycle_access_token()
        else:
            session = frame.session
        
        self.client.load_session(session)

    def freshen(self):
        if self.client.is_stale_session():
            frame = SessionFrame(self.identity)
            frame.cycle_access_token()
            self.client.load_session(frame.session)


    def get_profile(self):
        return self.client.get_profile(self.me)

    def map_profile(self, data):
        profile = data["profile"]
        identity = data["identity"]

        identity["profile_url"] = f"{self.BASE_URL}/profile/{profile['handle']}"
        identity["profile_image"] = profile.get("avatar", None)
        identity["username"] = profile["handle"]
        identity["name"] = profile.get("displayName", None)
        return identity
    
    
    # facets are Bluesky's approach to expressing hypertext linkages in post
    # content. We need to parse the post content and format data for Bluesky.
    def parse_facets(self, data):
        text = data["text"]
        facets = []

        for match in parse_mentions(text):
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
        embed = {}

        if metadata.get("link_card_draft_image") is not None:
            link_card = metadata["linkCard"]
            image = metadata["link_card_draft_image"]
            result = self.client.upload_blob(image)
            embed["external"] = {
                "uri": link_card.get("url", ""),
                "title": link_card.get("title", ""),
                "description": link_card.get("description", ""),
                "thumb": result["blob"]
            }

        images = []
        for attachment in post.get("attachments", []):
            result = self.client.upload_blob(attachment)
            images.append({
                "image": result["blob"],
                "alt": attachment.get("alt", "")
            })
        
        if len(images) > 0:
            embed["images"] = images

        if metadata.get("quote", None) is not None:
            embed["record"] = json.loads(metadata["quote"]["platform_id"])

        has_external = embed.get("external") is not None
        has_images = embed.get("images") is not None
        has_record = embed.get("record") is not None

        if has_record and has_external:
            embed = {
                "$type": "app.bsky.embed.recordWithMedia",
                "record": {
                    "$type": "app.bsky.embed.record",
                    "record": embed["record"]
                },
                "media": {
                    "$type": "app.bsky.embed.external",
                    "external": embed["external"]
                }
            }
        elif has_record and has_images:
            embed = {
                "$type": "app.bsky.embed.recordWithMedia",
                "record": {
                    "$type": "app.bsky.embed.record",
                    "record": embed["record"]
                },
                "media": {
                    "$type": "app.bsky.embed.images",
                    "images": embed["images"]
                }
            }
        elif has_record:
            embed["$type"] = "app.bsky.embed.record"
        elif has_images:
            embed["$type"] = "app.bsky.embed.images"
        elif has_external:
            embed["$type"] = "app.bsky.embed.external"
        else:
            embed = None
       

        reply = None
        if metadata.get("reply") is not None:
            uri = json.loads(metadata["reply"]["platform_id"])["uri"]
            parent = self.client.get_post(Post.parse_uri(uri))
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
            "$type": "app.bsky.feed.post",
            "createdAt": joy.time.now(),
            "text": metadata.get("text"),
            "facets": metadata.get("facets"),
        }

        if embed is not None:
            post_data["embed"] = embed
        if reply is not None:
            post_data["reply"] = reply

        logging.info(post_data)
        return self.client.create_post(post_data)



    def like_post(self, post):
        return self.client.like_post({
            "subject": json.loads(post["platform_id"]),
            "createdAt": joy.time.now(),
            "$type": "app.bsky.feed.like"
        })
    
    def undo_like_post(self, edge):
        rkey = Post.get_rkey(edge["stash"]["uri"])
        return self.client.undo_like_post(rkey)
    
    def repost_post(self, post):
        return self.client.repost_post({
            "subject": json.loads(post["platform_id"]),
            "createdAt": joy.time.now(),
            "$type": "app.bsky.feed.repost"
        })
    
    def undo_repost_post(self, edge):
        rkey = Post.get_rkey(edge["stash"]["uri"])
        return self.client.undo_repost_post(rkey)
    
    def remove_post(self, reference):
        rkey = Post.get_rkey(reference["uri"])
        return self.client.remove_post(rkey)
    
    def get_notification_posts(self, notifications):
        lookup = {}
        uris = set()
        for item in notifications:
            if item.post is not None:
                uris.add(item.post)

        if len(uris) > 0:
            # Note that https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/feed/getPosts.json#L16
            # says maxLength is 25, but empirically only up to 20 works.
            for sublist in h.partition(list(uris), 20):
                response = self.client.get_posts(sublist)
                for item in response["posts"]:
                    post = build_post(item)
                    # Discard reply edges, but retain shares.
                    post.reply = None
                    lookup[item["uri"]] = post
        
            for item in notifications:
                if item.post is not None:
                    value = lookup.get(item.post)
                    if value is None:
                        logging.warning(f"get_notification_posts: unable to fetch post {item.post} for notification attribution")
                    item.post = value

        return notifications

    
    def list_notifications(self, data):
        notifications = []
        cursor = None
        isDone = False
        last_retrieved = data.get("last_retrieved")
        is_active = True
        if last_retrieved is None:
            last_retrieved = h.two_weeks_ago()
            is_active = False

        # Special check to make sure that if our client was waiting in the
        # queue for a while, the access token hasn't become stale.
        self.freshen()
        
        while True:
          if isDone == True:
              break

          result = self.client.list_notifications(cursor)
          cursor = result.get("cursor")
          items = result.get("notifications", [])
          if len(items) == 0:
              break
          
          for item in items:
            notification = build_notification(item, is_active)
            if notification is None:
                continue
            if notification.created < last_retrieved:
                isDone = True
                break
            notifications.append(notification)
          
          # TODO: I had trouble getting the cursor to appear, but it's promised
          #   as part of the response. So pagination remains unconfirmed.
          if cursor is None:
              break

        self.get_notification_posts(notifications)

        actors = []
        seen_actors = set()
        partials = []
        seen_posts = set()
        for notification in notifications:
            actor = notification.author
            if actor is not None and actor.id not in seen_actors:
                actors.append(actor)
                seen_actors.add(actor.id)
            post = notification.post
            if post is not None and post.id not in seen_posts:
                partials.append(post)
                seen_posts.add(post.id)
                share = post.share
                if share is not None and share.id not in seen_posts:
                    partials.append(share)
                    seen_posts.add(share.id)

        for post in partials:
            actor = post.author
            if actor.id not in seen_actors:
                seen_actors.add(actor.id)
                actors.append(actor)
      
        return {
            "posts": [],
            "partials": partials,
            "actors": actors,
            "notifications": notifications
        }
    

    # Bluesky _does_ support the concept of reading a notification, and it's
    # present in the resource. However, notifications are not represented with
    # identifiers, so they cannot be individually referenced for update.
    # Instead, they offer a blanket dismissal with a time cursor, assuming
    # that you're dealing with a frontend that works like theirs. So we'll 
    # stub this for now and seek guidance on desired behavior.
    def dismiss_notification(self, notification):
        pass


    def map_notifications(self, data):
        notifications = []
        sources = {}
        for item in data["sources"]:
            sources[item["platform_id"]] = item
        posts = {}
        for item in data["posts"]:
            posts[item["platform_id"]] = item
        
        for notification in data["notifications"]:
            source_id = None
            post_id = None
            if notification.author is not None:
                source_id = sources[notification.author.id]["id"]
            if notification.post is not None:
                post_id = posts[notification.post.id]["id"]
            notifications.append({
                "platform": "bluesky",
                "platform_id": notification.id,
                "base_url": self.BASE_URL,
                "type": notification.type,
                "notified": notification.created,
                "active": notification.active,
                "source_id": source_id,
                "post_id": post_id
            })

        return notifications


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

            if post.thread is not None:
                for id in post.thread:
                    edges.append({
                        "origin_type": "post",
                        "origin_reference": post.id,
                        "target_type": "post",
                        "target_reference": id,
                        "name": "threads",
                    })

        return {
            "posts": posts,
            "partials": partials,
            "edges": edges
        }


    def get_post_graph(self, source, last_retrieved = None, is_shallow = False):
        # Special case for expired authorization.
        if self.invalid == True:
            return False
        
        # Special case for sources with invalid handles. Needs more work.
        if source["username"] == "handle.invalid":
            logging.warn("Bluesky get_post_graph: source has 'handle.invalid' username")
            return False
        
        # Special check to make sure that if our client was waiting in the
        # queue for a while, the access token hasn't become stale.
        self.freshen()

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
            default_limit = 200
        
        isDone = False
        count = 1
        while True:
            if isDone == True:
                break

            logging.info(f"Bluesky Fetch {source['username']} {cursor}")
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
            share = post.share
            if share is not None and share.id not in seen_posts:
                seen_posts.add(share.id)
                partials.append(share)

                share = share.share
                if share is not None and share.id not in seen_posts:
                    seen_posts.add(share.id)
                    partials.append(share)

            reply = post.reply
            if reply is not None:
                post.thread = []
                thread = build_thread(
                    self.client.get_thread(post.uri, 1000, 0)
                )
                for ancestor in thread:
                    post.thread.append(ancestor.id)
                    if ancestor.id not in seen_posts:
                        seen_posts.add(ancestor.id)
                        partials.append(ancestor)
      


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
