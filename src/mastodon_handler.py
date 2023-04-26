import gobo_types
from daemon_handler import *
import mastodon_utils as utils
from mastodon.errors import MastodonNotFoundError

import pdb


class MastodonHandler(DaemonHandler):
  def __init__(self):
    super(MastodonHandler, self).__init__()
  

  async def get_client(self, identity):
    return utils.get_client(identity.base_url, access_token = identity.oauth_token)

  async def close_client(self, client = None):
    return 
  
  async def get_identity_info(self, identity, client = None):
    if not client:
      client = await self.get_client(identity)

    user_info = client.me()
    updated_identity = gobo_types.identity_from_mastodon_user(user_info, identity.base_url, old_identity = identity)

    return updated_identity

  async def get_following(self, identity, client = None):
    if not client:
      client = await self.get_client(identity)
      
    user_id = client.me().id

    return [gobo_types.followed_source_from_mastodon_account(account, identity.base_url)
            for account in client.account_following(user_id, limit=None)]

  async def get_lists(self, identity: gobo_types.Identity, client = None):
    if not client:
      client = await self.get_client(identity)

    try:
      user_id = client.me().id
      return [gobo_types.list_from_mastodon_list(mastodon_list, identity.base_url)
              for mastodon_list in client.lists()]
    except MastodonNotFoundError:
      return []

  async def get_list_follows(self, identity: gobo_types.Identity, follow_list: gobo_types.List, client = None):
    if not client:
      client = await self.get_client(identity)

    try:
      return [gobo_types.followed_source_from_mastodon_account(account, identity.base_url)
              for account in client.list_accounts(follow_list.identifier)]
    except MastodonNotFoundError:
      return []

  async def fetch_posts(self, followed_source: gobo_types.FollowedSource, start_time, client) -> list:
    current_timestamp = datetime.datetime.now().timestamp()
    statuses = client.account_statuses(followed_source.identifier)
    posts = []

    while True:
      for toot in statuses:
        # If the current status is older than max age, break
        if current_timestamp - toot.created_at.timestamp() > self._max_age:
          break
        # Otherwise add it to the list
        posts = posts + [gobo_types.post_from_toot(toot, current_timestamp, followed_source)]

      # If there are no statuses or the oldest status is older than max age, break
      if not statuses:
        break

      oldest_toot = statuses[-1]
      if current_timestamp - oldest_toot.created_at.timestamp() > self._max_age:
        break
      
      # Otherwise, fill statuses with things older than the current oldest item
      statuses = client.account_statuses(followed_source.identifier, max_id = oldest_toot.id)

    return posts
