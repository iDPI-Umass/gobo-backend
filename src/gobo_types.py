from dataclasses import dataclass
from datetime import datetime

import pdb

@dataclass
class Identity:
  user_id: str = None
  base_url: str = None
  profile_url: str = None
  profile_image: str = None
  identity_id: int = None
  username: str = None
  display_name: str = None
  oauth_token: str = None
  oauth_token_secret: str = None
  last_updated: datetime = None

  def to_dict(self):
    return {"base_url": self.base_url,
            "profile_url": self.profile_url,
            "profile_image": self.profile_image,
            "identity_id": self.identity_id,
            "username": self.username,
            "display_name": self.display_name}
  def __lt__(self, other):
    return self.identity_id < other.identity_id
  

def identity_from_tweepy_user(user, old_identity: Identity = None):
  identity = Identity()
  identity.base_url = "twitter.com"
  identity.profile_url = "https://twitter.com/" + user.username
  identity.profile_image = user.profile_image_url
  identity.username = user.username
  identity.display_name = user.name

  if old_identity:
    identity.user_id = old_identity.user_id
    identity.identity_id = old_identity.identity_id
    identity.oauth_token = old_identity.oauth_token
    identity.oauth_token_secret = old_identity.oauth_token_secret

  return identity

def identity_from_redditor(user, old_identity: Identity = None):
  identity = Identity()
  identity.base_url = "www.reddit.com"
  identity.profile_url = "https://www.reddit.com/user/" + user.name
  identity.profile_image = user.icon_img
  identity.username = user.name

  if old_identity:
    identity.user_id = old_identity.user_id
    identity.identity_id = old_identity.identity_id
    identity.oauth_token = old_identity.oauth_token

  return identity
    
def identity_from_mastodon_user(user, base_url, old_identity: Identity = None):
  identity = Identity()
  identity.base_url = base_url
  identity.profile_url = user.url
  identity.profile_image = user.avatar
  identity.username = user.username
  identity.display_name = user.display_name

  if old_identity:
    identity.user_id = old_identity.user_id
    identity.identity_id = old_identity.identity_id
    identity.oauth_token = old_identity.oauth_token

  return identity

@dataclass
class BlockedKeyword:
  user_id: str = None
  word: str = None
  category: str = None

  def to_dict(self):
    return {"word": self.word,
            "category": self.category}

  
@dataclass
class UserProfile:
  user_id: str = None
  display_name: str = None

  def to_dict(self):
    return {"display_name": self.display_name}

  
@dataclass
class PendingRegistration:
  user_id: str = None
  base_url: str = None
  oauth_token: str = None
  oauth_token_secret: str = None
  saved_state: str = None
  request_time: datetime = None

@dataclass
class MastodonCredential:
  base_url: str = None
  client_id: str = None
  client_secret: str = None
  last_updated: datetime = None

@dataclass
class FollowedSource:
  source_id: int = None
  last_updated: datetime = None
  last_retrieved: datetime = None
  base_url: str = None
  identifier: str = None
  url: str = None
  username: str = None
  display_name: str = None
  icon_url: str = None
  active: bool = None

  def __lt__(self, other):
    return self.source_id < other.source_id
              
def by_source_id(element):
  return element.source_id

def by_list_id(element):
  return element.list_id
  

def followed_source_from_tweepy_user(user, old_source: FollowedSource = None):
  source = FollowedSource()
  source.base_url = "twitter.com"
  source.identifier = str(user.id)
  source.url = "https://twitter.com/" + user.username
  source.username = user.username
  source.display_name = user.name
  source.icon_url = user.profile_image_url
  source.active = True

  if old_source:
    source.source_id = old_source.source_id
  
  return source

def followed_source_from_subreddit(subreddit, old_source: FollowedSource = None):
  source = FollowedSource()
  source.base_url = "www.reddit.com"
  source.identifier = str(subreddit)
  source.url = "https://www.reddit.com/r/" + str(subreddit)
  source.display_name = str(subreddit)
  source.icon_url = subreddit.icon_img
  source.active = True

  if old_source:
    source.source_id = old_source.source_id
  
  return source

def followed_source_from_mastodon_account(account, base_url, old_source: FollowedSource = None):
  source = FollowedSource()
  source.base_url = base_url
  source.identifier = str(account.id)
  source.url = account.url
  source.icon_url = account.avatar
  if '@' in account.acct:
    source.username = account.acct
  else:
    source.username = account.username + '@' + base_url
  source.display_name = account.display_name
  source.active = True

  if old_source:
    source.source_id = old_source.source_id
  
  return source

@dataclass
class List:
 list_id: int = None
 user_id: str = None
 identity_id: int = None
 base_url: str = None
 url: str = None
 identifier: str = None
 visibility: str = None
 last_updated: datetime = None
 display_name: str = None

def list_from_tweepy_list(tweepy_list, old_list: List = None):
  new_list = List()

  new_list.base_url = "twitter.com"
  new_list.url = "https://twitter.com/i/lists/" + tweepy_list.id
  new_list.identifier = str(tweepy_list.id)
  if tweepy_list.private:
    new_list.visibility = "private"
  else:
    new_list.visibility = "public"

  new_list.display_name = tweepy_list.name
    
  if old_list:
    new_list.list_id = old_list.list_id
    new_list.user_id = old_list.user_id
    new_list.identity_id = old_list.identity_id

  return new_list

def list_from_mastodon_list(mastodon_list, base_url, old_list: List = None):
  new_list = List()

  new_list.base_url = base_url
  new_list.url = "https://" + base_url + "/lists/" + str(mastodon_list.id)
  new_list.identifier = str(mastodon_list.id)
  new_list.visibility = "private"
  new_list.display_name = mastodon_list.title

  if old_list:
    new_list.list_id = old_list.list_id
    new_list.user_id = old_list.user_id
    new_list.identity_id = old_list.identity_id

  return new_list

@dataclass
class Subscription:
 user_id: str = None
 identity_id: int = None
 list_id: int = None
 source_id: int = None
 active: bool = False
 is_new: bool = False # is_new is used for the process of updating subscriptions, it is not a column in the database

@dataclass
class Post:
  post_id: int = None # post_id is assigned by the database
  base_url: str = None
  identifier: str = None
  title: str = None
  content: str = None
  author: str = None
  uri: str = None
  visibility: str = None
  retrieved_at: datetime = None
  edited_at: datetime = None

def post_from_tweepy_tweet(tweet, includes, retrieved_at, source):
  post = Post()
  post.base_url = "twitter.com"
  post.identifier = str(tweet.id)
  post.title = None
  post.content = tweet.text
  for user in includes["users"]:
    if user.id == tweet.author_id:
      post.author = "https://twitter.com/" + user.username
      post.uri = "https://twitter.com/" + user.username + "/status/" + str(tweet.id)
  post.source_id = source.source_id
  return post
      
def post_from_reddit(submission, retrieved_at, source):
  post = Post()
  post.base_url = "www.reddit.com"
  post.identifier = submission.id
  post.title = submission.title
  if hasattr(post, 'selftext_html'):
    post.content = submission.selftext_html
  post.author = str(submission.author)
  post.uri = submission.permalink
  post.source_id = source.source_id
  return post

def post_from_toot(toot, retrieved_at, source):
  post = Post()
  post.base_url = source.base_url
  post.identifier = str(toot.id)
  post.title = None
  post.content = toot.content
  post.author = toot.account.url
  post.uri = toot.uri
  post.visibility = toot.visibility
  post.source_id = source.source_id
  
  return post

@dataclass
class Page:
  page_id: int = None # page_id is generated by the database
  identity_id: int = None
  newer_page_id: int = None
  older_page_id: int = None
  retrieved_before: datetime = None
  retrieved_after: datetime = None

@dataclass
class NewestPage:
  identity_id: int = None
  page_id: int = None
  
