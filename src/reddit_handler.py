import gobo_types
from daemon_handler import *

from os import environ
import asyncstdlib.itertools as it
import asyncpraw
import pdb


class RedditHandler(DaemonHandler):
  def __init__(self):
    super(RedditHandler, self).__init__()
  

  async def get_client(self, identity):
    client = asyncpraw.Reddit(
      refresh_token = identity.oauth_token,
      client_id = environ.get("REDDIT_CLIENT_ID"),
      client_secret = environ.get("REDDIT_CLIENT_SECRET"),
      redirect_uri = environ.get("OAUTH_CALLBACK_URL"),
      user_agent = environ.get("REDDIT_USER_AGENT"),
    )
    return client

  async def close_client(self, client = None):
    if client:
      await client.close()
  
  async def get_identity_info(self, identity, client = None):
    if not client:
      client = await self.get_client(identity)

    user_info = await client.user.me()
    updated_identity = gobo_types.identity_from_redditor(user_info, old_identity = identity)

    return updated_identity
    
  async def get_following(self, identity: gobo_types.Identity, client = None):
    """Retrieve the current information about who the input identity is following returns a list of gobo_types.FollowedSource"""
    if not client:
      client = await self.get_client(identity)
      
    return [gobo_types.followed_source_from_subreddit(subreddit) async for subreddit in client.user.subreddits(limit=None)]

  async def get_lists(self, identity: gobo_types.Identity, client = None):
    # Reddit does not have anything analagous to the lists feature that I can find
    return []

  async def get_list_follows(self, identity: gobo_types.Identity, follow_list: gobo_types.List, client = None):
    # Reddit does not have anything analagous to the lists feature that I can find
    return []

  async def fetch_posts(self, followed_source: gobo_types.FollowedSource, start_time, client) -> list:
    current_timestamp = datetime.datetime.now().timestamp()
    subreddit = await client.subreddit(followed_source.identifier, fetch=True)
    current_timestamp = datetime.datetime.now().timestamp()
    post_generator = it.takewhile(lambda post: current_timestamp - post.created_utc <= self._max_age, subreddit.new(limit=None))
    #async for post in post_generator:
    #  self.post_queue.put_nowait(post)

    posts = [gobo_types.post_from_reddit(submission, current_timestamp, followed_source) async for submission in post_generator]
    return posts
