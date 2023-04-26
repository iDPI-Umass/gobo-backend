import tweepy
from datetime import datetime
from db_utils import insert_pending_registration, insert_identity
from os import environ

def start_add_twitter_identity(user_id):
  # Create the user handler
  user_handler = tweepy.OAuth1UserHandler(environ.get("TWITTER_CONSUMER_KEY"),
                                          environ.get("TWITTER_CONSUMER_SECRET"),
                                          callback = environ.get("OAUTH_CALLBACK_URL"))

  # Get the authorization URL
  redirect_url = user_handler.get_authorization_url()

  # Store the pending registration
  temp_oauth_token = user_handler.request_token["oauth_token"]
  temp_oauth_secret = user_handler.request_token["oauth_token_secret"]

  insert_pending_registration(user_id,
                              "twitter.com",
                              oauth_token = temp_oauth_token,
                              oauth_token_secret = temp_oauth_secret)

  return redirect_url

# Retrieves a more permanent oauth key and saves it to identities
# Assumes that the request has already been sanitized
def continue_add_twitter_identity(user_id, request, pending):
  user_handler = tweepy.OAuth1UserHandler(environ.get("TWITTER_CONSUMER_KEY"),
                                          environ.get("TWITTER_CONSUMER_SECRET"))

  user_handler.request_token = {
    "oauth_token": pending.oauth_token,
    "oauth_token_secret": pending.oauth_token_secret
  }
    
  # Get tokens
  access_token, access_token_secret = user_handler.get_access_token(request.args["oauth_verifier"])

  # Get user info
  client = get_client(access_token, access_token_secret)
  user_info = client.get_me(user_fields=["name", "username", "profile_image_url"]).data
    
  # Add to identities
  insert_identity(user_id = user_id,
                  base_url = "twitter.com",
                  profile_url = "https://twitter.com/" + user_info.username,
                  profile_image = user_info.profile_image_url,
                  username = user_info.username,
                  display_name = user_info.name,
                  oauth_token = access_token,
                  oauth_token_secret = access_token_secret)

  # Don't fail silently

  
def get_client(oauth_token, oauth_token_secret):
  return tweepy.Client(
    consumer_key = environ.get("TWITTER_CONSUMER_KEY"),
    consumer_secret = environ.get("TWITTER_CONSUMER_SECRET"),
    access_token = oauth_token,
    access_token_secret = oauth_token_secret
  )
