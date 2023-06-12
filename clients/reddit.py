import logging
from os import environ
import praw


class Reddit():
    def __init__(self, identity = None):
        self.identity = identity or {}
        self.client = praw.Reddit(
            refresh_token = self.identity.get("oauth_token"),
            client_id = environ.get("REDDIT_CLIENT_ID"),
            client_secret = environ.get("REDDIT_CLIENT_SECRET"),
            user_agent = environ.get("REDDIT_USER_AGENT"),
            redirect_uri = environ.get("OAUTH_CALLBACK_URL")
        )


    def get_redirect_url(self, state):
        return self.client.auth.url(
            scopes = ["identity", "mysubreddits", "read"],
            state=state,
            duration = "permanent"
        )

    def convert_code(self, code):
        return self.client.auth.authorize(code)

    def get_profile(self):
        return self.client.user.me()