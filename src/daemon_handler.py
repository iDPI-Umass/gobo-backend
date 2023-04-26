import gobo_types
import pdb
import asyncio
import datetime
from psycopg_pool import AsyncConnectionPool
from db_utils import async_update_identity, async_insert_update_followed_sources, async_get_source_subscriptions, async_insert_update_lists, update_subscriptions, async_get_list_subscriptions, find_identity, update_source_last_retrieved

class DaemonHandler:
  def __init__(self):
    # TODO(slane): Move these parameters to a .env file or other config file
    self._max_age = 60 * 60 * 24 * 2 # 2 days for now
    
  async def get_client(self, identity: gobo_types.Identity):
    raise Exception("Not implemented")

  async def get_identity_info(self, identity: gobo_types.Identity, client = None):
    raise Exception("Not implemented")

  async def get_following(self, identity: gobo_types.Identity, client = None):
    raise Exception("Not implemented")

  async def get_lists(self, identity: gobo_types.Identity, client = None):
    raise Exception("Not implemented")

  async def get_list_follows(self, identity: gobo_types.Identity, follow_list: gobo_types.List, client = None):
    raise Exception("Not implemented")
  
  async def close_client(self, client = None):
    raise Exception("Not implemented")

  async def fetch_posts(self, followed_source: gobo_types.FollowedSource, start_time, client = None) -> list:
    raise Exception("Not implemented")
  
  async def update_identity_info(self, pool: AsyncConnectionPool, identity: gobo_types.Identity):
    """Update subscriptions, profile picture, etc"""
    client = await self.get_client(identity)
    
    # Update identity info
    updated_identity = await self.get_identity_info(identity, client)
    await async_update_identity(pool, updated_identity)

    await self._update_source_subscriptions(pool, client, identity = identity)

    # Get set of lists that this identity is following or made
    lists = await self.get_lists(identity, client)

    # Update the set of lists and get list_ids
    lists = await async_insert_update_lists(pool, lists)
    
    # Sort by list_id for later
    lists.sort(key = gobo_types.by_list_id)
    
    # Update list source subscriptions
    for follow_list in lists:
      await self._update_source_subscriptions(pool, client, identity = identity, follow_list = follow_list)

    # Update the set of list subscriptions for this identity
    #pdb.set_trace()
    await self._update_list_subscriptions(pool, client, identity = identity, lists = lists)
    
    await self.close_client(client)

  async def _update_source_subscriptions(self,
                                         pool: AsyncConnectionPool,
                                         client,
                                         identity: gobo_types.Identity = None,
                                         follow_list: gobo_types.List = None):
    #pdb.set_trace()
    if not follow_list:
      # Get set of sources that this identity is following directly
      following = await self.get_following(identity, client)
      identity_id = identity.identity_id
      list_id = None
    else:
      # Get set of sources that the input list is following
      following = await self.get_list_follows(identity, follow_list, client)
      identity_id = None
      list_id = follow_list.list_id
      

    following = await async_insert_update_followed_sources(pool, following)
    following.sort(key = gobo_types.by_source_id)
    old_subscriptions = await async_get_source_subscriptions(pool, identity_id, list_id)
    
    to_update = await self._build_subscriptions_to_update(following = following,
                                                         old_subscriptions = old_subscriptions,
                                                         identity_id = identity_id,
                                                         list_id = list_id)
    # Insert or update each of the elements of to_update
    await update_subscriptions(pool, to_update)

  async def _build_subscriptions_to_update(self,
                                           following: list = None,
                                           old_subscriptions: list = None,
                                           identity_id: int = None,
                                           list_id: int = None):
    """Find the sources that are present in old_subscriptions but not in following"""
    to_update = []
    subscription_index = 0
    following_index = 0
    while subscription_index < len(old_subscriptions) or following_index < len(following):
      if subscription_index >= len(old_subscriptions):
        # Everything else in the list is just in following. They are new and should be active
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = list_id,
                                                 source_id = following[following_index].source_id,
                                                 active = True,
                                                 is_new = True))
        following_index += 1
      elif following_index >= len(following):
        # Everything else in the list is just in old_subscriptions. They are not new and should be deavtivated
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = list_id,
                                                 source_id = old_subscriptions[subscription_index].source_id,
                                                 active = False,
                                                 is_new = False))
        subscription_index += 1
      elif old_subscriptions[subscription_index].source_id == following[following_index].source_id:
        # Value is in both lists, it isn't new and should be active
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = list_id,
                                                 source_id = following[following_index].source_id,
                                                 active = True,
                                                 is_new = False))
        subscription_index += 1
        following_index += 1
      elif old_subscriptions[subscription_index].source_id < following[following_index].source_id:
        # Value is in old_subscriptions but not following. It should be deactivated and isn't new
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = list_id,
                                                 source_id = old_subscriptions[subscription_index].source_id,
                                                 active = False,
                                                 is_new = False))
        subscription_index += 1
      else:
        # Value is in following but not old_subscriptions. It's new and should be active
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = list_id,
                                                 source_id = following[following_index].source_id,
                                                 active = True,
                                                 is_new = True))
        following_index += 1

    return to_update

  async def _update_list_subscriptions(self,
                                       pool: AsyncConnectionPool,
                                       client,
                                       identity: gobo_types.Identity = None,
                                       lists: list = None):
    # Get old subscriptions
    old_subscriptions = await async_get_list_subscriptions(pool, identity.identity_id)
    
    # Build subscription to update
    to_update = await self._build_list_subscriptions_to_update(lists, old_subscriptions, identity.identity_id)
    
    # Insert them into database
    await update_subscriptions(pool, to_update)
    
  async def _build_list_subscriptions_to_update(self,
                                               following: list = None,
                                               old_subscriptions: list = None,
                                               identity_id: int = None):
    to_update = []
    subscription_index = 0
    following_index = 0
    while subscription_index < len(old_subscriptions) or following_index < len(following):
      if subscription_index >= len(old_subscriptions):
        # Everything else in the list is just in following. They are new and should be active
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = following[following_index].list_id,
                                                 active = True,
                                                 is_new = True))
        following_index += 1
      elif following_index >= len(following):
        # Everything else in the list is just in old_subscriptions. They are not new and should be deavtivated
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = old_subscriptions[subscription_index].list_id,
                                                 active = False,
                                                 is_new = False))
        subscription_index += 1
      elif old_subscriptions[subscription_index].list_id == following[following_index].list_id:
        # Value is in both lists, it isn't new and should be active
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = following[following_index].list_id,
                                                 active = True,
                                                 is_new = False))
        subscription_index += 1
        following_index += 1
      elif old_subscriptions[subscription_index].list_id < following[following_index].list_id:
        # Value is in old_subscriptions but not following. It should be deactivated and isn't new
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = old_subscriptions[subscriptions_index].list_id,
                                                 active = False,
                                                 is_new = False))
        subscription_index += 1
      else:
        # Value is in following but not old_subscriptions. It's new and should be active
        to_update.append(gobo_types.Subscription(identity_id = identity_id,
                                                 list_id = following[following_index].list_id,
                                                 active = True,
                                                 is_new = True))
        following_index += 1

    return to_update    
    
  async def process_source(self,  pool: AsyncConnectionPool, followed_source: gobo_types.FollowedSource, post_queue: asyncio.Queue):
    retrieval_time = datetime.datetime.now()
    # Select a random identity that is subscribed to this source
    identity = await find_identity(pool, followed_source)

    # Create a client usign that identity
    client = await self.get_client(identity)

    # Calculate time at which to start looking
    # Either current time - max age or last retrieved, whichever is sooner
    # Note: Right now, only the Twitter Daemon uses this. Mastodon and Reddit both
    # Update existing posts up to _max_age in the past
    start_time = datetime.datetime.now() - datetime.timedelta(seconds=self._max_age)
    start_timestamp = start_time.timestamp()

    if followed_source.last_retrieved and followed_source.last_retrieved.timestamp() > start_timestamp:
      start_time = followed_source.last_retrieved

    # Fetch posts
    posts = await self.fetch_posts(followed_source, start_time, client)

    # Add posts to post queue
    for post in posts:
      post_queue.put_nowait(post)

    print("Post Queue contains {} items".format(post_queue.qsize()))

    await self.close_client(client)
    
    # Update last retrieved
    await update_source_last_retrieved(pool, followed_source, retrieval_time)
  
