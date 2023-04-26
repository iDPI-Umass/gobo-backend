from flask import Flask, request, jsonify, Response
from flask_cors import cross_origin, CORS
from functools import wraps
from auth_utils import get_token_auth_header, get_token_payload, check_permissions
from urllib.parse import urlparse
from db_utils import insert_pending_registration, get_pending_registration, clear_pending_registration, get_identity_info, delete_identity, get_blocked_keywords, insert_blocked_keywords, delete_blocked_keywords, get_user_profile, put_user_profile, get_fresh_feed, get_page, get_recent_posts
from reddit_utils import start_add_reddit_identity, continue_add_reddit_identity
from twitter_utils import start_add_twitter_identity, continue_add_twitter_identity
from mastodon_utils import start_add_mastodon_identity, continue_add_mastodon_identity
import pdb
from errors import AuthError, handle_auth_error, ArgError, handle_arg_error
from validation_utils import sanitize_add_identity, sanitize_add_identity_callback, validate_pending_registration, sanitize_remove_identity, sanitize_blocked_keywords, sanitize_user_profile

from dotenv import load_dotenv
 
load_dotenv()

app = Flask(__name__)
CORS(app)

def requires_auth(f):
  """Determines if the Access Token is valid
  """
  @wraps(f)
  def decorated(*args, **kwargs):
    token = get_token_auth_header()
    payload = get_token_payload(token)
    request.current_user = payload
    
    return f(*args, **kwargs)
  return decorated

app.register_error_handler(AuthError, handle_auth_error)
app.register_error_handler(ArgError, handle_arg_error)

@app.route("/")
def hello_world():
  return "<p>Hello, World!</p>"

@app.route("/add-identity", methods=['GET', 'POST'])
@requires_auth
def add_identity_route():
  check_permissions(request.current_user, "general")
  if request.method == 'GET':
    return add_identity()
  elif request.method == 'POST':
    return add_identity_callback()
  else:
    return "Invalid HTTP request method", 400

def add_identity():
  sanitize_add_identity(request)

  user_id = request.current_user["sub"]
  base_url = request.args["base_url"].lower()
  
  # If reddit
  if base_url == "www.reddit.com":
    redirectURL = start_add_reddit_identity(user_id)
  # If twitter
  elif base_url == "twitter.com":
    redirectURL = start_add_twitter_identity(user_id)
  # If mastodon
  else:
    redirectURL = start_add_mastodon_identity(user_id, base_url)

  response = jsonify(redirectURL = redirectURL)
  response.status_code = 200
  return response

def add_identity_callback():
  sanitize_add_identity_callback(request)
  user_id = request.current_user["sub"]
  
  # Use user ID to get pending registrtation
  pending = get_pending_registration(user_id)

  base_url = request.args["base_url"].lower()
  
  # Verify that pending registration exists and that the info matches
  is_valid, err = validate_pending_registration(request, pending)
  if not is_valid:
    response = Response(response = "Error: " + err["msg"], status = err["status"])
  else:
    # If reddit
    if base_url == "www.reddit.com":
      continue_add_reddit_identity(user_id, request)
      response = Response(response = "Success", status = 201)
    # If twitter
    elif base_url == "twitter.com":
      continue_add_twitter_identity(user_id, request, pending)
      response = Response(response = "Success", status = 201)
    # If mastodon
    else:
      continue_add_mastodon_identity(user_id, base_url, request)
      response = Response(response = "Success", status = 201)

  clear_pending_registration(user_id)
  return response

@app.route("/identity-info", methods=['GET'])
@requires_auth
def identity_info():
  check_permissions(request.current_user, "general")
  user_id = request.current_user["sub"]
  identities = get_identity_info(user_id)
  response = jsonify({"identities": identities})
  response.status_code = 200

  return response

@app.route("/remove-identity", methods=['DELETE'])
@requires_auth
def remove_identity():
  check_permissions(request.current_user, "general")
  sanitize_remove_identity(request)
  user_id = request.current_user["sub"]
  identity_id = request.args["identity_id"]
  delete_identity(user_id, identity_id)
  return "Success", 200

@app.route("/fresh-feed", methods=['GET'])
@requires_auth
def fresh_feed():
  check_permissions(request.current_user, "general")
  user_id = request.current_user["sub"]
  #return get_fresh_feed(user_id)
  posts = get_recent_posts(user_id)
  response = jsonify({"posts": posts})
  response.status_code = 200
  return response

@app.route("/page", methods=['GET'])
@requires_auth
def page():
  check_permissions(request.current_user, "general")
  # Check that this user owns this page
  sanitize_get_page(request)
  user_id = request.current_user["sub"]
  page_id = request.args["page_id"]
  return get_page(user_id, page_id)

@app.route("/create-post", methods=['POST'])
@requires_auth
def create_post():
  check_permissions(request.current_user, "general")
  return "Not yet implemented", 501

@app.route("/blocked-keywords", methods=['GET', 'POST', 'DELETE'])
@requires_auth
def blocked_keywords_route():
  check_permissions(request.current_user, "general")
  if request.method == "GET":
    return blocked_keywords_get()
  elif request.method == "POST":
    return blocked_keywords_post()
  elif request.method == "DELETE":
    return blocked_keywords_delete()
  else:
    return "Invalid HTTP request method", 400

def blocked_keywords_get():
  user_id = request.current_user["sub"]
  result = get_blocked_keywords(user_id)
  response = jsonify({"keywords": result})
  response.status_code = 200
  return response
  
def blocked_keywords_post():
  sanitize_blocked_keywords(request)
  return insert_blocked_keywords(request.current_user["sub"], request.args["word"], request.args["category"])
  
def blocked_keywords_delete():
  sanitize_blocked_keywords(request)
  delete_blocked_keywords(request.current_user["sub"], request.args["word"], request.args["category"])
  return "Success", 200


@app.route("/user-profile", methods=['GET', 'PUT'])
@requires_auth
def user_profile_route():
  check_permissions(request.current_user, "general")
  if request.method == "GET":
    return user_profile_get()
  elif request.method == "PUT":
    return user_profile_put()
  else:
    return "Invalid HTTP request method", 400

def user_profile_get():
  result = get_user_profile(request.current_user["sub"])
  response = jsonify(result)
  response.status_code = 200
  return response

def user_profile_put():
  sanitize_user_profile(request)
  put_user_profile(request.current_user["sub"], request.args["display_name"])
  return "Success", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0')
