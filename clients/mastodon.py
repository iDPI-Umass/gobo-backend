import logging
from os import environ
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

        if _.reblog is not None:
            self.reblog = Status(_.reblog)
        if _.reblog is not None and _.url is None:
            self.url = self.reblog.url

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
                  value = poll.expires_at
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
        edges = []
        for status in data["statuses"]:
            if status.id is None:
                continue

            source = sources[status.account.id]

            post = {
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

            posts.append(post)

            if status.reblog != None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": status.id,
                    "target_type": "post",
                    "target_reference": status.reblog.id,
                    "name": "shares",
                })

        return {
            "posts": posts,
            "edges": edges
        }


    def list_sources(self):
        id = self.identity["platform_id"]
        items = self.client.account_following(id, limit=None)
        
        accounts = []
        for item in items:
            accounts.append(Account(item))

        return {"accounts": accounts}


    def get_post_graph(self, source):
        isDone = False
        last_retrieved = source.get("last_retrieved")
        max_id = None

        statuses = []
        accounts = []

        count = 1
        while True:
            if isDone == True:
                break

            items = self.client.account_statuses(
                source["platform_id"],
                max_id = max_id
            )

            if len(items) == 0:
                break

            max_id = str(items[-1].id)

            if last_retrieved == None:
                for item in items:
                    statuses.append(Status(item))
                    count += 1
                    if count >= 1000:
                        isDone = True
                        break
            else:
                for item in items:
                    status = Status(item)
                    if status.published > last_retrieved:
                        statuses.append(status)
                    else:
                        isDone = True
                        break


        seen_statuses = set()
        for status in statuses:
            reblog = status.reblog
            if reblog != None and reblog.id not in seen_statuses:
                seen_statuses.add(reblog.id)
                statuses.append(reblog)


        seen_accounts = set()
        for status in statuses:
            account = status.account
            if account.id not in seen_accounts:
                seen_accounts.add(account.id)
                accounts.append(account)

        return {
            "statuses": statuses,
            "accounts": accounts
        }