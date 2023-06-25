import logging
from os import environ
import tweepy
import joy
import models
from .helpers import guess_mime, md, partition


class Tweet():
    def __init__(self, _):
        self._ = _
        self.id = _.id
        self.author_id = _.author_id
        self.content = _.text
        self.published = joy.time.to_iso_string(_.created_at)
        self.media_keys = (_.attachments or {}).get("media_keys") or []
        self.quote_tweet_ids = []

        for tweet in self._.referenced_tweets or []:
            if tweet.type in ["quoted"]:
                self.quote_tweet_ids.append(tweet.id)

class User():
    def __init__(self, _):
        self._ = _
        self.id = _.id
        self.username = _.username
        self.name = _.name
        self.icon_url = _.profile_image_url

class Media():
    def __init__(self, _):
        self._ = _
        self.url = _.url
        self.media_key = _.media_key

        if self.url != None:
            self.content_type = guess_mime(self.url)
        else:
            best = None
            for variant in _.variants:
                if variant.get("bit_rate") == None:
                    continue
                if best == None or variant["bit_rate"] > best["bit_rate"]:
                    best = variant

            self.url = _.url or best["url"]
            self.content_type = best["content_type"]
                

            


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

  
    def get_profile(self):
        user_fields = ["name", "username", "profile_image_url"]
        return self.client.get_me(user_fields=user_fields).data

    def get_post(self, id):
        _tweet = self.client.get_tweet(id, 
            user_auth=True,
            expansions=[
                "author_id",
                "attachments.media_keys",
                "attachments.poll_ids",
                "referenced_tweets.id",
                "referenced_tweets.id.author_id",
            ],
            tweet_fields=["created_at"],
            user_fields=["profile_image_url"],
            media_fields=["url", "variants"]
        )

        tweet = Tweet(_tweet.data)
        logging.info(tweet.id)
        logging.info(tweet.content)
        logging.info(tweet.media_keys)
        logging.info(tweet.referenced_tweets)
        logging.info(tweet.published)
        return tweet

    def map_sources(self, data):
        base_url = Twitter.BASE_URL
        sources = []
        for user in data["users"]:
            sources.append({
                "platform_id": user.id,
                "base_url": base_url,
                "url": f"{base_url}/{user.username}",
                "username": user.username,
                "name": user.name,
                "icon_url": user.icon_url,
                "active": True
            })
  
        return sources


    def map_posts(self, data):
        base_url = Twitter.BASE_URL

        tweets = {}
        for item in data["tweets"]:
            tweets[item.id] = tweet

        sources = {}
        for item in data["sources"]:
            sources[item["platform_id"]] = item

        media = {}
        for item in data["media"]:
            media[item.media_key] = item
        
        posts = []
        edges = []
        for tweet in data["tweets"]:
            source = sources[tweet.author_id]
            tweet_url = f"{source['url']}/status/{tweet.id}"

            attachments = []
            for key in tweet.media_keys:
                file = media[key]
                attachments.append({
                    "url": file.url,
                    "type": file.content_type
                })

            posts.append({
                "source_id": source["id"],
                "base_url": base_url,
                "platform_id": tweet.id,
                "title": None,
                "content": tweet.content,
                "url": tweet_url,
                "published": tweet.published,
                "attachments": attachments
            })

            for id in tweet.quote_tweet_ids:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": tweet.id,
                    "target_type": "post",
                    "target_reference": id,
                    "name": "shares",
                })


        return {
            "posts": posts,
            "edges": edges
        }


    def list_sources(self):
        pages = tweepy.Paginator(
            self.client.get_users_following,
            self.identity["platform_id"],
            user_auth= True,
            user_fields = ["name", "username", "profile_image_url"]
        )
    
        users = []
        for page in pages:
            for user in page.data:
                users.append(User(user))

        return {"users": users}


    def get_post_graph(self, source):
        last_retrieved = None
        #last_retrieved = source.get("last_retrieved")
        if last_retrieved == None:
            max_pages = 1
        else:
            max_pages = 32

        pages = tweepy.Paginator(
            self.client.get_users_tweets,
            id = source["platform_id"],
            max_results=10,
            limit = max_pages,
            start_time = last_retrieved, 
            user_auth=True,
            expansions=[
                "author_id",
                "attachments.media_keys",
                "attachments.poll_ids",
                "referenced_tweets.id",
                "referenced_tweets.id.author_id",
            ],
            tweet_fields=["created_at", "entities"],
            user_fields=["profile_image_url"],
            media_fields=["url", "variants"]
        )
    
        seen_tweets = set()
        seen_users = set()
        seen_media = set()

        tweet_list = []
        user_list = []
        media_list = []
        for page in pages:
            if page.data == None:
                continue
            
            tweets = page.data
            users = page.includes.get("users") or []
            files = page.includes.get("media") or []
            
            
            for item in tweets:
                tweet = Tweet(item)
                if tweet.id not in seen_tweets:
                    seen_tweets.add(tweet.id)
                    tweet_list.append(tweet)
            for item in users:
                user = User(item)
                if user.id not in seen_users:
                    seen_users.add(user.id)
                    user_list.append(user)
            for item in files:
                file = Media(item)
                if file.url not in seen_media:
                    seen_media.add(file.url)
                    media_list.append(file)




        secondary = set()
        for tweet in tweet_list:
            for id in tweet.quote_tweet_ids:
                if id not in seen_tweets:
                    secondary.add(id)

        registered = models.post.pull([
            models.helpers.where("base_url", Twitter.BASE_URL),
            models.helpers.where("platform_id", list(secondary), "in")
        ])

        for item in registered:
            secondary.remove(item["profile_id"])
        
        if len(secondary) > 0:
            for sublist in list(partition(list(secondary), 100)):
                response = self.client.get_tweets(
                    sublist,
                    expansions=[
                        "author_id",
                        "attachments.media_keys",
                        "attachments.poll_ids",
                        "referenced_tweets.id.author_id",
                    ],
                    tweet_fields=["created_at"],
                    user_fields=["profile_image_url"],
                    media_fields=["url", "variants"]
                )

                tweets = page.data
                users = response.includes.get("users") or []
                files = resposne.includes.get("media") or []

                for item in tweets:
                    tweet = Tweet(item)
                    if tweet.id not in seen_tweets:
                        seen_tweets.add(tweet.id)
                        tweet_list.append(tweet)
                for item in users:
                    user = User(item)
                    if user.id not in seen_users:
                        seen_users.add(user.id)
                        user_list.append(user)
                for item in files:
                    file = Media(item)
                    if file.url not in seen_media:
                        seen_media.add(file.url)
                        media_list.append(file)




        return {
            "tweets": tweet_list,
            "users": user_list,
            "media": media_list
        }