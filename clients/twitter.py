import logging
from os import environ
import tweepy
import joy


class Twitter():
    BASE_URL = "https://twitter.com"

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

    @staticmethod
    def map_sources(data):
        sources = []
        for user in data["users"]:
            sources.append({
                "platform_id": str(user.id),
                "base_url": self.BASE_URL,
                "url": f"{self.BASE_URL}/{user.username}",
                "username": user.username,
                "name": user.name,
                "icon_url": user.profile_image_url,
                "active": True
            })
  
        return sources


    @staticmethod
    def map_posts(source, data):
        users = data["users"]
        posts = []
        for tweet in data["tweets"]:
            author = users[tweet.author_id]
            platform_id = str(tweet.id)
            post = {
                "source_id": source["id"],
                "base_url": self.BASE_URL,
                "platform_id": platform_id,
                "title": None,
                "content": tweet.text,
                "author": f"{self.BASE_URL}/{author.username}",
                "url": f"{self.BASE_URL}/{author.username}/status/{platform_id}",
            }
            posts.append(post)

        return posts


    def get_profile(self):
        user_fields = ["name", "username", "profile_image_url"]
        return self.client.get_me(user_fields=user_fields).data


    def list_sources(self):
        pages = tweepy.Paginator(
            client.get_users_following,
            self.identity.platform_id,
            user_auth= True,
            user_fields = ["name", "username", "profile_image_url"]
        )
    
        users = []
        for response in pages:
            users.extend(response.data)

        return {"users": users}


    def list_posts(self, source):
        pages = tweepy.Paginator(
            self.client.get_users_tweets,
            id = source["platform_id"],
            max_results=100, 
            start_time = joy.time.now(), 
            user_auth=True,
            expansions=['author_id', 'attachments.media_keys'],
            tweet_fields=['created_at', 'entities'],
            user_fields=['profile_image_url'],
            media_fields=['url']
        )
    

        tweets = []
        users = {}
        for response in pages:
            tweets.extend(response.data)
            for user in response.includes["users"]:
                users[user.id] = user
      
        return {
            "tweets": tweets,
            "users": users
        }