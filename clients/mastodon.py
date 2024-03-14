import logging
import time
from os import environ
from datetime import timedelta
from urllib.parse import urlparse
import re
import mastodon
import joy
import models
import clients.helpers as h 

# TODO: Do we want this to be moved into GOBO configuration?
redirect_uris = [
    "http://localhost:5117/add-identity-callback",
    "http://gobo.social/add-identity-callback",
    "https://gobo.social/add-identity-callback"
]

smalltown_urls = [
    "https://community.publicinfrastructure.org"
]

def build_status(item):
    try:
        status = Status(item)
        if status.visibility not in Status.VISIBLE:
          return None
    
        return status
    
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(item)
        logging.error("\n\n")
        return None
    
def build_partial_status(item):
    status = build_status(item)
    if status is not None:
        status.reply = None
    return status


class Status():
    VISIBLE = ["public", "unlisted", "followers only"]

    def __init__(self, _):
        self._ = _
        self.id = str(_.id)
        self.account = Account(_.account)
        self.content = _.content
        self.url = _.url
        self.visibility = self.get_visibility(_.visibility)
        self.published = joy.time.convert(
            start = "date",
            end = "iso",
            value = _.created_at
        )
        self.attachments = []
        self.poll = None
        self.reblog = None
        self.reply = None
        self.thread = None

        if _.reblog is not None:
            self.reblog = Status(_.reblog)
        if _.in_reply_to_id is not None:
            self.reply = str(_.in_reply_to_id)
        if self.url is None and self.reblog is not None:
            self.url = self.reblog.url
        if self.url is not None and self.url.endswith("/activity"):
            self.url = re.sub("\/activity$", "", self.url)

        if _.card is not None:
            self.attachments.append({
                "type": "application/json+gobo-syndication",
                "source": _.card["url"],
                "title": _.card["title"],
                "description": _.card["description"],
                "media": _.card.get("image", None)
            })

        for attachment in _.media_attachments:
            url = attachment["url"]
            self.attachments.append({
                "url": url,
                "type": h.guess_mime(url)
            })
          
        poll = getattr(_, "poll", None)
        if poll != None:
            self.poll = {
              "total": poll.votes_count,
              "ends": joy.time.convert(
                  start = "date",
                  end = "iso",
                  value = poll.expires_at,
                  optional = True
              ),
              "options": []
            }

            for option in poll.options:
                self.poll["options"].append({
                    "key": option.title,
                    "count": option.votes_count or 0
                })

    def get_visibility(self, type):
        if type == "public":
            return "public"
        if type == "unlisted":
            return "unlisted"
        if type == "private":
            return "followers only"
        return type
               

class Account():
    def __init__(self, _):
        self.id = str(_.id)
        self.url = _.url
        self.username = Account.get_username(_)
        self.name = _.display_name
        self.icon_url = _.avatar
        self.platform = Account.get_platform(self)

    @staticmethod
    def get_username(_):
        username = _.acct
        hostname = urlparse(_.url).hostname
        if username is None:
            return username
        if "@" in username:
            return username
        else:
            return username + "@" + hostname
        
    @staticmethod
    def get_platform(account):
        base_url = h.get_base_url(account.url)
        if base_url in smalltown_urls:
            return "smalltown"
        else:
            return "mastodon"

class Poll():
    def __init(self, _):
        self.id = _.id


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
        self.id = str(_.id)
        self.created = joy.time.convert(
            start = "date",
            end = "iso",
            value = _.created_at
        )
        self.active = is_active
        self.account = None
        self.status = None
        
        if getattr(_, "account", None) is not None:
            self.account = Account(_.account)
        if getattr(_, "status", None) is not None:
            self.status = build_partial_status(_.status)
        
        self.type = self.map_type(_.type, self.status)


    def map_type(self, type, status):
        if type == "follow":
            return "follow"
        if type == "follow_request":
            return "follow request"
        if type == "favourite":
            return "like"
        if type == "reblog":
            return "repost"
        if type == "mention":
            if status is None:
              return "direct message"
            else:
              return "mention"              
        if type == "poll":
            return "poll complete"
        if type == "status":
            return "new post"
        logging.warning(f"Mastodon: unable to map notification type {type}")
        return type


class Mastodon():
    def __init__(self, identity = None):
        self.identity = identity
        self.invalid = False
        if self.identity is not None:
            self.base_url = self.identity["base_url"]

    @staticmethod
    def register_client(base_url):
        client_id, client_secret = mastodon.Mastodon.create_app(
            "gobo.social",
            scopes = ['read', 'write'],
            redirect_uris = redirect_uris,
            website = "https://gobo.social",
            api_base_url = base_url
        )

        return {
          "base_url": base_url,
          "client_id": client_id,
          "client_secret": client_secret
        }
    

    def login(self):
        base_url = self.base_url
        mastodon_client = models.mastodon_client.find({"base_url": base_url})
        if mastodon_client == None:
            raise Exception(f"no mastodon client found for {base_url}")

        self.client = mastodon.Mastodon(
            client_id = mastodon_client["client_id"],
            client_secret = mastodon_client["client_secret"],
            api_base_url = mastodon_client["base_url"],
            access_token = self.identity.get("oauth_token")
        )

    def get_redirect_url(self, state):
        return self.client.auth_request_url(
            redirect_uris = environ.get("OAUTH_CALLBACK_URL"),
            scopes = ['read', 'write'],
            force_login=True,
            state = state
        )

    def convert_code(self, code):
        return self.client.log_in(
            code = code,
            redirect_uri = environ.get("OAUTH_CALLBACK_URL"),
            scopes = ['read', 'write']
        )

    def get_profile(self):
        return self.client.me()
    
    def map_profile(self, data):
        profile = data["profile"]
        identity = data["identity"]

        identity["profile_url"] = profile.url
        identity["profile_image"] = profile.avatar
        identity["username"] = profile.username
        identity["name"] = profile.display_name
        return identity

    
    def create_post(self, post, metadata):
        media_ids = []
        for draft in post.get("attachments", []):
            result = self.upload_media(draft)
            media_ids.append(result["id"])

        allowed_visibility = [ "public", "private", "direct", "unlisted" ]
        visibility = metadata.get("visibility", "public")
        if visibility not in allowed_visibility:
            raise Exception(f"visibility {visibility} is invalid")
        
        reply = None
        if metadata.get("reply", None) is not None:
            reply = metadata["reply"]["platform_id"]

        return self.client.status_post(
            status = post.get("content", ""),
            idempotency_key = joy.crypto.random({"encoding": "safe-base64"}),
            media_ids = media_ids,
            sensitive = metadata.get("sensitive", False),
            spoiler_text = metadata.get("spoiler", None),
            visibility = visibility,
            in_reply_to_id = reply,
            # TODO: Do we want to include langauge metadata?
            # language=None,
            # TODO: Do we want to include polls?
            # poll=None
        )
    
    def upload_media(self, draft):
        return self.client.media_post(
            media_file = draft["data"],
            mime_type = draft["mime_type"],
            description = draft["alt"],
            focus = (0, 0)
        )
    
    def favourite_post(self, post):
        return self.client.status_favourite(post["platform_id"])
    
    def undo_favourite_post(self, post):
        return self.client.status_unfavourite(post["platform_id"])
    
    def boost_post(self, post):
        return self.client.status_reblog(post["platform_id"])
    
    def undo_boost_post(self, post):
        return self.client.status_unreblog(post["platform_id"])
    
    def get_notifications(self, data):
        return self.client.notifications(
            max_id = data.get("max_id"), 
            limit = data.get("limit"),
            types = data.get("type", [ 
                "follow",
                "follow_request", 
                "favourite",
                "reblog",
                "mention",
                "poll",
                "status"
            ])
        )
    
    def list_notifications(self, data):
        notifications = []
        max_id = None
        isDone = False
        last_retrieved = data.get("last_retrieved")
        is_active = True
        if last_retrieved is None:
            last_retrieved = h.two_weeks_ago()
            is_active = False
        
        while True:
          if isDone == True:
              break

          items = self.get_notifications({"max_id": max_id})
          if len(items) == 0:
              break
          max_id = str(items[-1].id)

          for item in items:
            notification = build_notification(item, is_active)
            if notification is None:
                continue
            if notification.created < last_retrieved:
                isDone = True
                break
            notifications.append(notification)

        accounts = []
        seen_accounts = set()
        partials = []
        seen_statuses = set()
        for notification in notifications:
            account = notification.account
            if account is not None and account.id not in seen_accounts:
                accounts.append(account)
                seen_accounts.add(account.id)
            status = notification.status
            if status is not None and status.id not in seen_statuses:
                partials.append(status)
                seen_statuses.add(status.id)
                reblog = status.reblog
                if reblog is not None and reblog.id not in seen_statuses:
                    seen_statuses.add(reblog.id)
                    partials.append(reblog)

        for status in partials:
            account = status.account
            if account.id not in seen_accounts:
                seen_accounts.add(account.id)
                accounts.append(account)
      
        return {
            "statuses": [],
            "accounts": accounts,
            "partials": partials,
            "notifications": notifications
        }
    

    # Mastodon doesn't seem to have a concept of reading a notification. Their
    # API describes a dismissal resource that uses POST, but it deletes the
    # notification. Stub this for now.
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
            if notification.account is not None:
                source_id = sources[notification.account.id]["id"]
            if notification.status is not None:
                post_id = posts[notification.status.id]["id"]
            notifications.append({
                "platform": notification.account.platform,
                "platform_id": notification.id,
                "base_url": self.base_url,
                "type": notification.type,
                "notified": notification.created,
                "active": notification.active,
                "source_id": source_id,
                "post_id": post_id
            })

        return notifications


    def map_sources(self, data):
        sources = []
        for account in data["accounts"]:
            sources.append({
                "platform": account.platform,
                "platform_id": account.id,
                "base_url": self.base_url,
                "url": account.url,
                "username": account.username,
                "name": account.name,
                "icon_url": account.icon_url,
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

        def map_post(source, status):
            return {
                "source_id": source["id"],
                "base_url": source["base_url"],
                "platform": source["platform"],
                "platform_id": status.id,
                "title": None,
                "content": status.content,
                "visibility": status.visibility,
                "url": status.url,
                "published": status.published,
                "attachments": status.attachments,
                "poll": status.poll
            }


        for status in data["statuses"]:
            if status.id is None:
                continue
            source = sources[status.account.id]
            posts.append(map_post(source, status))


        for status in data["partials"]:
            if status.id is None:
                continue
            source = sources[status.account.id]
            partials.append(map_post(source, status))

        
        for status in (data["statuses"] + data["partials"]):
            if status.reblog is not None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": status.id,
                    "target_type": "post",
                    "target_reference": status.reblog.id,
                    "name": "shares",
                })

            if status.reply is not None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": status.id,
                    "target_type": "post",
                    "target_reference": status.reply,
                    "name": "replies",
                })

            if status.thread is not None:
                for id in status.thread:
                    edges.append({
                        "origin_type": "post",
                        "origin_reference": status.id,
                        "target_type": "post",
                        "target_reference": id,
                        "name": "threads",
                    })


        return {
            "posts": posts,
            "partials": partials,
            "edges": edges
        }


    def list_sources(self):
        accounts = []
        logging.info("Mastodon: Fetching self profile data")
        accounts.append(Account(self.get_profile()))

        id = self.identity["platform_id"]
        max_id = None
        while True:
            logging.info(f"Mastodon: Fetching following page {max_id}")
            items = self.client.account_following(
                id = id,
                max_id = max_id, 
                limit = 80
            )
            
            if len(items) == 0:
                break
              
            for item in items:
                accounts.append(Account(item))

            page_data = getattr(items[-1], "_pagination_next", None)
            if page_data is None:
                break
            for key, value in page_data.items():
                if key == "max_id":
                    max_id = str(value)
                    break

        return {"accounts": accounts}
    
    @staticmethod
    def is_expired_token(error):
        junk, status, status_description, message = error.args
        return status == 401 and message == "The access token is invalid"


    def get_post_graph(self, source, last_retrieved = None, is_shallow = False):
        # Special case for expired authorization.
        if self.invalid == True:
            return False
        
        isDone = False
        oldest_limit = joy.time.convert("date", "iso", 
            joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
        )
        if is_shallow == True:
            default_limit = 40
        else:
            default_limit = 100
        max_id = None
        platform_id = source["platform_id"]
        visibilities = ["public", "unlisted"]

        statuses = []
        partials = []
        accounts = []

        count = 1
        while True:
            if isDone == True:
                break

            logging.info(f"Mastdon Fetch {source['username']}: {platform_id} {max_id}")
            try:
                items = self.client.account_statuses(
                    id = platform_id,
                    max_id = max_id,
                    limit=40
                )
            except mastodon.errors.MastodonUnauthorizedError as e:
                if Mastodon.is_expired_token(e):
                    logging.warning(e, exc_info=True)
                    self.invalid = True
                    return False


            if len(items) == 0:
                break

            max_id = str(items[-1].id)

            if last_retrieved == None:
                for item in items:
                    status = build_status(item)
                    if status is None:
                        continue
                    
                    count += 1
                    if status.published < oldest_limit:
                        isDone = True
                        break
                    if count < default_limit:
                        statuses.append(status)
                    else:
                        isDone = True
                        break
            else:
                for item in items:
                    status = build_status(item)
                    if status is None:
                        continue
                    
                    if status.published < oldest_limit:
                        isDone = True
                        break
                    if status.published > last_retrieved:
                        statuses.append(status)
                    else:
                        isDone = True
                        break


        seen_statuses = set()
        for status in statuses:
            seen_statuses.add(status.id)
        
        for status in statuses: 
            reblog = status.reblog
            if reblog is not None and reblog.id not in seen_statuses:
                seen_statuses.add(reblog.id)
                partials.append(reblog)

        for status in statuses:
            reply = status.reply
            if reply is not None:
                try:
                    logging.info(f"Mastodon: fetching context for {status.id}")
                    context = self.client.status_context(status.id)
                    status.thread = []
                    for item in context.ancestors:
                        ancestor = build_status(item)
                        if ancestor is None:
                            continue
                        status.thread.append(ancestor.id)
                        if ancestor.id not in seen_statuses:
                            seen_statuses.add(ancestor.id)
                            partials.append(ancestor)
            
                except Exception as e:
                    logging.warning(f"failed to fetch context {status.id} {e}")



        seen_accounts = set()
        for status in statuses:
            account = status.account
            if account.id not in seen_accounts:
                seen_accounts.add(account.id)
                accounts.append(account)
        for status in partials:
            account = status.account
            if account.id not in seen_accounts:
                seen_accounts.add(account.id)
                accounts.append(account)



        return {
            "statuses": statuses,
            "partials": partials,
            "accounts": accounts
        }