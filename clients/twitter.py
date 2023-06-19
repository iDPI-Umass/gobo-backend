import logging
from os import environ
import tweepy
import markdown
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
        base_url = Twitter.BASE_URL
        sources = []
        for user in data["users"]:
            sources.append({
                "platform_id": str(user.id),
                "base_url": base_url,
                "url": f"{base_url}/{user.username}",
                "username": user.username,
                "name": user.name,
                "icon_url": user.profile_image_url,
                "active": True
            })
  
        return sources


    @staticmethod
    def map_posts(source, data):
        base_url = Twitter.BASE_URL
        users = data["users"]
        posts = []
        for tweet in data["tweets"]:
            author = users[tweet.author_id]
            author_url = f"{base_url}/{author.username}"
            platform_id = str(tweet.id)

            attachments = []
            for attachment in tweet.attachments:
                for key in attachment["media_keys"]:
                    attachments.append(media[key].url)

            post = {
                "source_id": source["id"],
                "base_url": base_url,
                "platform_id": platform_id,
                "title": None,
                "content": markdown.markdown(tweet.text),
                "author": author_url,
                "url": f"{author_url}/status/{platform_id}",
                "published": tweet.created_at,
                "attachments": attachments
            }
            posts.append(post)

        return posts


    def get_profile(self):
        user_fields = ["name", "username", "profile_image_url"]
        return self.client.get_me(user_fields=user_fields).data


    def list_sources(self):
        pages = tweepy.Paginator(
            self.client.get_users_following,
            self.identity["platform_id"],
            user_auth= True,
            user_fields = ["name", "username", "profile_image_url"]
        )
    
        users = []
        for response in pages:
            if response.data == None:
                continue
            users.extend(response.data)

        return {"users": users}


    def list_posts(self, source):
        last_retrieved = source.get("last_retrieved")
        if last_retrieved == None:
            max_pages = 1
        else:
            max_pages = 32

        pages = tweepy.Paginator(
            self.client.get_users_tweets,
            id = source["platform_id"],
            max_results=100,
            limit = max_pages,
            start_time = last_retrieved, 
            user_auth=True,
            expansions=['author_id', 'attachments.media_keys'],
            tweet_fields=['created_at', 'entities'],
            user_fields=['profile_image_url'],
            media_fields=['url']
        )
    

        tweets = []
        users = {}
        media = {}
        for response in pages:
            if response.data == None:
                continue
            tweets.extend(response.data)
            for user in response.includes["users"]:
                users[user.id] = user
            for medium in response.includes["media"]:
                media[medium.media_key] = medium


        return {
            "tweets": tweets,
            "users": users,
            "media": media
        }