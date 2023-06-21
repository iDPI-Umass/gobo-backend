import logging
from os import environ
import mastodon
import joy
from .helpers import guess_mime

# TODO: Do we want this to be moved into GOBO configuration?
redirect_uris = [
    "http://localhost:5117/add-identity-callback",
    "http://gobo.social/add-identity-callback",
    "https://gobo.social/add-identity-callback"
]


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
                "username": account.acct,
                "name": account.display_name,
                "icon_url": account.avatar,
                "active": True
            })
  
        return sources


    def map_posts(self, source, data):        
        posts = []
        for status in data["statuses"]:
            content = status.content
            attachments = []

            if status.reblog != None:
                content = f"</p>Reblog from @{status.reblog['account']['acct']}:</p>"
                content = content + status.reblog.content

                for attachment in status.reblog.media_attachments:
                    url = attachment["url"]
                    attachments.append({
                        "url": url,
                        "type": guess_mime(url)
                    })
            else:
                for attachment in status.media_attachments:
                    url = attachment["url"]
                    attachments.append({
                        "url": url,
                        "type": guess_mime(url)
                    })

            post = {
                "source_id": source["id"],
                "base_url": self.base_url,
                "platform_id": status.id,
                "title": None,
                "content": content,
                "url": status.url,
                "published": joy.time.to_iso_string(status.created_at),
                "attachments": attachments
            }

            posts.append(post)

        return posts


    def list_sources(self):
        id = self.identity["platform_id"]
        accounts = self.client.account_following(id, limit=None)
        return {"accounts": accounts}


    def list_posts(self, source):
        isDone = False
        last_retrieved = source.get("last_retrieved")
        max_id = None
        toots = []

        while True:
            if isDone == True:
                break

            statuses = self.client.account_statuses(
                source["platform_id"],
                max_id = max_id
            )

            max_id = statuses[-1].id

            if last_retrieved == None:
                toots = statuses
                isDone = True
            else:
                for status in statuses:
                    timestamp = joy.time.to_iso_string(status.created_at)
                    if timestamp > last_retrieved:
                        toots.append(status)
                    else:
                        isDone = True
                        break
        
        return {
            "statuses": toots,
            "accounts": []
        }