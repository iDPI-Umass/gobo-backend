import logging
import tweepy
from os import environ


class Twitter():
    def __init__(self, identity):
        self.identity = identity
        self.client = tweepy.Client(
            consumer_key = environ.get("TWITTER_CONSUMER_KEY"),
            consumer_secret = environ.get("TWITTER_CONSUMER_SECRET"),
            access_token = identity["oauth_token"],
            access_token_secret = identity["oauth_token_secret"],
            wait_on_rate_limit = True
        )

    @staticmethod
    def get_user_handler(registration):
        handler = tweepy.OAuth1UserHandler(
            environ.get("TWITTER_CONSUMER_KEY"),
            environ.get("TWITTER_CONSUMER_SECRET"),
            callback = environ.get("OAUTH_CALLBACK_URL")
        )

        if registration != None:
            handler.request_token = {
                "oauth_token": registration["oauth_token"],
                "oauth_token_secret": registration["oauth_token_secret"]
            }

        return handler


    def get_profile(self):
        user_fields = ["name", "username", "profile_image_url"]
        return self.client.get_me(user_fields=user_fields).data

    def list_posts(self, source):
    current_timestamp = datetime.datetime.now().timestamp()
        pages = tweepy.Paginator(
            self.client.get_user_tweets,
            id = followed_source.identifier,
            max_results=100, start_time = start_time, user_auth=True,
            expansions=['author_id', 'attachments.media_keys'],
            tweet_fields=['created_at', 'entities'],
            user_fields=['profile_image_url'],
            media_fields=['url'])
    

    posts = []
    for response in pages:
      if response.data:
        posts = posts + [gobo_types.post_from_tweepy_tweet(tweet, response.includes, current_timestamp, followed_source) for tweet in response.data]
  
    return posts