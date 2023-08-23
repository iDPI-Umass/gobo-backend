import logging
from os import environ
import re
import mastodon
import joy
import models
from .helpers import guess_mime

# TODO: Do we want this to be moved into GOBO configuration?
redirect_uris = [
    "http://localhost:5117/add-identity-callback",
    "http://gobo.social/add-identity-callback",
    "https://gobo.social/add-identity-callback"
]

def build_status(item):
    try:
        return Status(item)
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(item)
        logging.error("\n\n")
        return None

class Status():
    def __init__(self, _):
        self._ = _
        self.id = str(_.id)
        self.account = Account(_.account)
        self.content = _.content
        self.url = _.url
        self.published = joy.time.convert(
            start = "date",
            end = "iso",
            value = _.created_at
        )
        self.attachments = []
        self.poll = None
        self.reblog = None
        self.reply = None

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
                "type": guess_mime(url)
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
               

class Account():
    def __init__(self, _):
        self.id = str(_.id)
        self.url = _.url
        self.username = _.acct
        self.name = _.display_name
        self.icon_url = _.avatar

class Poll():
    def __init(self, _):
        self.id = _.id
        


class Mastodon():
    def __init__(self, mastodon_client, identity = None):
        self.identity = identity or {}
        self.base_url = mastodon_client["base_url"]
        self.client = mastodon.Mastodon(
            client_id = mastodon_client["client_id"],
            client_secret = mastodon_client["client_secret"],
            api_base_url = mastodon_client["base_url"],
            access_token = self.identity.get("oauth_token")
        )

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
    
    def create_post(self, post, metadata):
        media_ids = []
        for draft in post.get("attachments", []):
            result = self.upload_media(draft)
            media_ids.append(result["id"])

        allowed_visibility = [ "public", "private", "direct", "unlisted" ]
        visibility = metadata.get("visibility", "public")
        if visibility not in allowed_visibility:
            raise Exception(f"visibility {visibility} is invalid")


        return self.client.status_post(
            status = post.get("content", ""),
            idempotency_key = joy.crypto.random({"encoding": "safe-base64"}),
            media_ids = media_ids,
            sensitive = metadata.get("sensitive", False),
            spoiler_text = metadata.get("spoiler", None),
            visibility = visibility,
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

    def map_sources(self, data):
        base_url = self.base_url
        sources = []
        for account in data["accounts"]:
            sources.append({
                "platform_id": account.id,
                "base_url": base_url,
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
                "base_url": self.base_url,
                "platform_id": status.id,
                "title": None,
                "content": status.content,
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


        return {
            "posts": posts,
            "partials": partials,
            "edges": edges
        }


    def list_sources(self):
        id = self.identity["platform_id"]
        items = self.client.account_following(id, limit=None)
        
        accounts = []
        accounts.append(Account(self.get_profile()))
        for item in items:
            accounts.append(Account(item))

        return {"accounts": accounts}


    def get_post_graph(self, source):
        isDone = False
        last_retrieved = source.get("last_retrieved", None)
        max_id = None

        statuses = []
        partials = []
        accounts = []

        count = 1
        while True:
            if isDone == True:
                break

            logging.info(f"Mastdon Fetch: {source['platform_id']} {max_id}")
            items = self.client.account_statuses(
                source["platform_id"],
                max_id = max_id,
                limit=40
            )

            if len(items) == 0:
                break

            max_id = str(items[-1].id)

            if last_retrieved == None:
                for item in items:
                    status = build_status(item)
                    if status is None:
                        continue
                    
                    statuses.append(status)
                    count += 1
                    if count >= 400:
                        isDone = True
                        break
            else:
                for item in items:
                    status = build_status(item)
                    if status is None:
                        continue
                    
                    if status.published > last_retrieved:
                        statuses.append(status)
                    else:
                        isDone = True
                        break


        seen_statuses = set()
        reply_ids = set()
        for status in statuses:
            seen_statuses.add(status.id)
        
        for status in statuses: 
            reblog = status.reblog
            if reblog is not None and reblog.id not in seen_statuses:
                seen_statuses.add(reblog.id)
                partials.append(reblog)

        for status in statuses:
            reply = status.reply
            if reply is not None and reply not in seen_statuses:
                seen_statuses.add(reply)
                reply_ids.add(reply) 



        registered = models.post.pull([
            models.helpers.where("base_url", source["base_url"]),
            models.helpers.where("platform_id", list(reply_ids), "in")
        ])

        for item in registered:
            reply_ids.remove(item["platform_id"])

        for id in reply_ids:
            try:
                logging.info(f"Mastodon: fetching reply {id}")
                status = Status(self.client.status(id))
                # We need to stop traversing the graph so we don't pull in lots of replies.
                # By definition, Mastodon does not allow replies to boosted statuses.
                status.reply = None
                partials.append(status)
            except Exception as e:
                logging.warning(f"failed to fetch status {id} {e}")



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