from mastodon import Mastodon
import random
from db_utils import insert_pending_registration, insert_identity, get_mastodon_credentials, insert_mastodon_credentials
import pdb

from os import environ

def start_add_mastodon_identity(user_id, base_url):
  client = get_client(base_url)

  state = str(random.randint(0, 65000))
  redirect_url = client.auth_request_url(redirect_uris = environ.get("OAUTH_CALLBACK_URL"),
                                         scopes = ['read', 'write'],
                                         force_login=True,
                                         state = state)

  insert_pending_registration(user_id, base_url, saved_state=state)

  print(redirect_url)
  
  return redirect_url

def continue_add_mastodon_identity(user_id, base_url, request):
  client = get_client(base_url)
  access_token = client.log_in(code = request.args["code"],
                               redirect_uris = environ.get("OAUTH_CALLBACK_URL"),
                               scopes = ['read', 'write'])
  
  user = client.me()

  insert_identity(user_id = user_id,
                  base_url = base_url,
                  profile_url = user.url,
                  profile_image = user.avatar,
                  username = user.username,
                  display_name = user.display_name,
                  oauth_token = access_token)


def get_client(base_url, access_token = None):
  client_id, client_secret = get_credentials(base_url)
  client = Mastodon(client_id = client_id, client_secret = client_secret, api_base_url = base_url, access_token=access_token)
  
  return client


def get_credentials(base_url):
  # Check DB for existing credentials for base URL
  entry = get_mastodon_credentials(base_url)
  
  # If credentials don't exist, request them then add to database
  if entry:
    client_id = entry.client_id
    client_secret = entry.client_secret
  else:
    client_id, client_secret = Mastodon.create_app("gobo.social",
                                                   scopes=['read', 'write'],
                                                   redirect_uris=["http://localhost:5117/add-identity-callback",
                                                                  "http://gobo.social/add-identity-callback",
                                                                  "https://gobo.social/add-identity-callback"],
                                                   website="https://gobo.social/",
                                                   api_base_url = base_url)

    insert_mastodon_credentials(base_url, client_id, client_secret)
  return client_id, client_secret
