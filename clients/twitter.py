import logging
from os import environ
import tweepy
import snscrape.modules.twitter as sns
import joy
import models
from .helpers import guess_mime, md, partition


class Tweet():
    def __init__(self, _):
        self._ = _
        self.id = str(_.id)
        self.user = User(_.user)
        self.content = _.rawContent
        self.url = _.url
        self.published = joy.time.convert(
            start = "date",
            end = "iso",
            value = _.date
        )
        self.attachments = []
        self.poll = None
        self.share = None


        media = _.media or []
        for item in media:
            if type(item) == sns.Photo:
                self.attachments.append({
                    "url": item.fullUrl,
                    "type": "image/jpeg"
                })
            elif type(item) == sns.Video or type(item) == sns.Gif:
                best = None
                for variant in item.variants:
                    if best == None:
                        best = variant
                    else:
                        best_bitrate = best.bitrate or 0
                        variant_bitrate = variant.bitrate or 0
                        if best_bitrate < variant_bitrate:
                            best = variant
                self.attachments.append({
                    "url": best.url,
                    "type": best.contentType
                })

        if type(_.card) == sns.PollCard:
            self.poll = {
                "total": 0,
                "ends": joy.time.convert(
                    start = "date",
                    end = "iso",
                    value = _.card.endDate
                ),
                "options": []
            }

            for option in _.card.options:
                self.poll["total"] = self.poll["total"] + (option.count or 0)
                self.poll["options"].append({
                    "key": option.label,
                    "count": option.count or 0
                })


        share = None
        if getattr(_, "retweetedTweet", None) != None:
            share = _.retweetedTweet
        if getattr(_, "quotedTweet", None) != None:
            share = _.quotedTweet
        if share != None and type(share) != sns.TweetRef:
            self.share = Tweet(share)

    def __repr__(self):
        return str({
          "id": self.id,
          "user": self.user,
          "content": self.content,
          "url": self.url,
          "published": self.published,
          "attachments": self.attachments,
          "poll": self.poll,
          "share": self.share,
        })


class User():
    def __init__(self, _):
        self._ = _
        self.id = str(_.id)
        self.username = _.username
        self.name = _.displayname
        self.icon_url = _.profileImageUrl

    def __repr__(self):
        return str({
          "id": self.id,
          "username": self.username,
          "name": self.name,
          "icon_url": self.icon_url
        })


            


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

    def list_posts(self, user):
        generator = sns.TwitterSearchScraper(
            query = f"from:{user}",
            mode = sns.TwitterSearchScraperMode.LIVE
        ).get_items()

        for i, tweet in enumerate(generator):
            t = Tweet(tweet)
            logging.info(t)

            if i > 10:
                break

    def get_user(self, name):
        profile = sns.TwitterProfileScraper(name)
        for name in dir(profile.entity):
            if not name.startswith("__"):
                logging.info(f"{name} {getattr(profile.entity, name)}")


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

        sources = {}
        for item in data["sources"]:
            sources[item["platform_id"]] = item
        
        posts = []
        edges = []
        for tweet in data["tweets"]:
            if tweet.id is None:
                continue

            source = sources[tweet.user.id]

            posts.append({
                "source_id": source["id"],
                "base_url": base_url,
                "platform_id": tweet.id,
                "title": None,
                "content": tweet.content,
                "url": tweet.url,
                "published": tweet.published,
                "attachments": tweet.attachments,
                "poll": tweet.poll
            })

            if tweet.share != None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": tweet.id,
                    "target_type": "post",
                    "target_reference": tweet.share.id,
                    "name": "shares",
                })


        return {
            "posts": posts,
            "edges": edges
        }


    def list_sources(self):
        users = [
            User(sns.TwitterProfileScraper("meakoopa").entity),
            User(sns.TwitterProfileScraper("PlayStation").entity),
            User(sns.TwitterProfileScraper("EthanZ").entity),
        ]

        return {"users": users}


    def get_post_graph(self, source):
        last_retrieved = source.get("last_retrieved")

        tweets = []
        users = []

        generator = sns.TwitterSearchScraper(
            query = f"from:{source['username']}",
            mode = sns.TwitterSearchScraperMode.LIVE
        ).get_items()

        if last_retrieved == None:
            for i, item in enumerate(generator):
                tweet = Tweet(item)
                if i < 25:
                    tweets.append(tweet)
                else:
                    break
        else:
            for item in generator:
                tweet = Tweet(item)
                if tweet.published > last_retrieved:
                    tweets.append(item)
                else:
                    break


        seen_tweets = set()
        for tweet in tweets:
            share = tweet.share
            if share != None and share.id not in seen_tweets:
                seen_tweets.add(share.id)
                tweets.append(share)


        seen_users = set()
        for tweet in tweets:
            user = tweet.user
            if user.id not in seen_users:
                seen_users.add(user.id)
                users.append(user)


        return {
            "tweets": tweets,
            "users": users
        }