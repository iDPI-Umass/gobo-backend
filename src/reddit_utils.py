import praw
import random
from db_utils import insert_pending_registration, insert_identity
from os import environ

def start_add_reddit_identity(user_id):
  client = get_client()

  state = str(random.randint(0, 65000))
  oauth_url = client.auth.url(scopes = ["identity", "mysubreddits", "read"], state=state, duration = "permanent")

  insert_pending_registration(user_id,
                              "www.reddit.com",
                              saved_state=state)
  
  return oauth_url

def continue_add_reddit_identity(user_id, request):
  # Get the code from the request
  code = request.args["code"]

  # Get a client
  client = get_client()
  
  # Convert code to token
  refresh_token = client.auth.authorize(code)
  
  # Use tokens to get user info
  authed_client = get_client(refresh_token)

  redditor = authed_client.user.me()
  username = redditor.name
  profile_image = redditor.icon_img
  
  
  # Add user info to database
  insert_identity(user_id = user_id,
                  base_url = "www.reddit.com",
                  profile_url = "https://www.reddit.com/user/" + username,
                  profile_image = profile_image,
                  username = username,
                  oauth_token = refresh_token)
  
def get_client(refresh_token = None):
  print(environ.get("REDDIT_USER_AGENT"))
  client = praw.Reddit(
    refresh_token = refresh_token,
    client_id = environ.get("REDDIT_CLIENT_ID"),
    client_secret = environ.get("REDDIT_CLIENT_SECRET"),
    redirect_uri = environ.get("OAUTH_CALLBACK_URL"),
    user_agent = environ.get("REDDIT_USER_AGENT"),
  )
  return client
