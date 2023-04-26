import gobo_types
from daemon_handler import *

import tweepy
from os import environ
import pdb

class TwitterHandler(DaemonHandler):
  def __init__(self):
    super(TwitterHandler, self).__init__()
  
  async def get_client(self, identity = None):
    if identity:
      client = tweepy.Client(consumer_key = environ.get("TWITTER_CONSUMER_KEY"),
                             consumer_secret = environ.get("TWITTER_CONSUMER_SECRET"),
                             access_token = identity.oauth_token,
                             access_token_secret = identity.oauth_token_secret,
                             wait_on_rate_limit = True)
      return client

  async def close_client(self, client = None):
    return
    
  async def get_identity_info(self, identity: gobo_types.Identity, client = None):
    """Retrieve the current information about the input identity returns gobo_types.Identity"""
    if not client:
      client = await self.get_client(identity)

    user_info = client.get_me(user_fields=["name", "username", "profile_image_url"])
    updated_identity = gobo_types.identity_from_tweepy_user(user_info.data, old_identity = identity)

    return updated_identity
    

  async def get_following(self, identity: gobo_types.Identity, client = None):
    """Retrieve the current information about who the input identity is following returns a list of gobo_types.FollowedSource"""
    if not client:
      client = await self.get_client(identity)
 
    user_id = client.get_me().data.id

    pages =  tweepy.Paginator(client.get_users_following,
                              user_id,
                              user_auth= True,
                              user_fields = ["name", "username", "profile_image_url"])
    subscriptions = []
    for response in pages:
      subscriptions = subscriptions + [gobo_types.followed_source_from_tweepy_user(source) for source in response.data]

    return subscriptions
      
  async def get_lists(self, identity: gobo_types.Identity, client = None):
    if not client:
      client = await self.get_client(identity)
 
    user_id = client.get_me().data.id

    pages =  tweepy.Paginator(client.get_followed_lists,
                              user_id,
                              user_auth = True,
                              list_fields = ["private"])
   
    lists = []
    for response in pages:
      lists = lists + [gobo_types.list_from_tweepy_list(tweepy_list) for tweepy_list in response.data]

    pages =  tweepy.Paginator(client.get_owned_lists,
                              user_id,
                              user_auth = True,
                              list_fields = ["private"])
    for response in pages:
      lists = lists + [gobo_types.list_from_tweepy_list(tweepy_list) for tweepy_list in response.data]

    return lists

  async def get_list_follows(self, identity: gobo_types.Identity, follow_list: gobo_types.List, client = None):
    if not client:
      client = await self.get_client(identity)

    pages = tweepy.Paginator(client.get_list_members,
                             follow_list.identifier,
                             user_auth = True,
                             user_fields = ["name", "username", "profile_image_url"])
        

    subscriptions = []
    for response in pages:
      subscriptions = subscriptions + [gobo_types.followed_source_from_tweepy_user(source) for source in response.data]

    return subscriptions
      

  async def fetch_posts(self, followed_source: gobo_types.FollowedSource, start_time, client) -> list:
    current_timestamp = datetime.datetime.now().timestamp()
    pages = tweepy.Paginator(client.get_users_tweets, id = followed_source.identifier,
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
