from errors import ArgError
import re

# add-identity endpoint requires:
#   base_url: string, URL of the given media source
def sanitize_add_identity(request):
  sanitize_base_url(request)
  
# add-identity endpoint requires:
#   base_url: string, URL of the given media source
#   One of:
#     oauth_token and oauth_validator
#     code and state
#     code
def sanitize_add_identity_callback(request):
  sanitize_base_url(request)

  if request.args["base_url"] == "twitter.com":
    if not "oauth_token" in request.args or not "oauth_verifier" in request.args:
      ArgError({"code": "invalid_query",
                    "description": "add-identity-callback for Twitter requires oauth_token and oauth_verifier in query"},
                   400)
  elif request.args["base_url"] == "www.reddit.com":
    if not "state" in request.args or not "code" in request.args:
      ArgError({"code": "invalid_query",
                    "description": "add-identity-callback for Reddit requires state and code in query"},
                   400)
  else:
    if not "code" in request.args:
      ArgError({"code": "invalid_query",
                    "description": "add-identity-callback for Mastodon requires code in query"},
                   400)

# base_url should either be an ipv4 address or domain name
# This method ensure that it is one of those and that the argument is present
def sanitize_base_url(request):
  if not "base_url" in request.args:
    raise ArgError({"code": "invalid_query",
                    "description": "Endpoint requires base_url argument"},
                   400)
  base_url = request.args["base_url"].lower()

  is_url = re.match("^([a-z0-9]+[.])+([a-z0-9])+$", base_url)
  is_ipv4 = re.match("^(?:[0-9]{1,3}\.){3}[0-9]{1-3}$", base_url)
  if not is_url and not is_ipv4:
    ArgError({"code": "invalid_query",
              "description": "base_url must either be an ipv4 address or a domain name (e.g. gobo.social or www.reddit.com)"},
             400)

# Ensures that the input identity information matches what is expected
# Must be run after sanitization
# Returns is_valid, msg where the message is an error message if there is an error
# If 
def validate_pending_registration(request, pending):
  if not pending:
    return False, {"msg": "No pending registration matching this user", "status": 400}
  
  if request.args["base_url"].lower() != pending.base_url:
    return False, {"msg": "base_url in query does not match base_url in pending identity registration", "status": 400}

  # Twitter should include oauth_token and oauth_verifier, oauth_token must match pending value
  if pending.base_url == "twitter.com":
    # Make sure the oauth_token values are present and available in both locations
    if not pending.oauth_token:
      return False, {"msg": "oauth_token required for Twitter identity registration but not present in pending registration",
                     "status": 500}
    elif not "oauth_token" in request.args:
      return False, {"msg": "oauth_token required for Twitter identity registration but not present in query",
                     "status": 400}
    # Make sure both oauth_token values match
    elif request.args["oauth_token"] != pending.oauth_token:
      return False, {"msg": "oauth_token in query does not match oauth_token in pending identity registration",
                     "status": 400}

    # Make sure the query has an oauth_verifier
    if not "oauth_verifier" in request.args:
      return False, {"msg": "oauth_verifier required for Twitter identity registration but not present in query",
                     "status": 400}

  # Reddit and Mastodon should include state and code, state must match pending value
  else:
    if not "state" in request.args:
      return False, {"msg": "state required for " + base_url + " identity registration but not present in query",
                    "status": 400}
    elif not pending.saved_state:
      return False, {"msg": "state required for " + base_url + " identity registration but not present in pending registration",
                     "status": 500}
    elif request.args["state"] != pending.saved_state:
      return False, {"msg": "state in query does not match oauth_token in pending identity registration",
                     "status": 400}
    if not "code" in request.args:
      return False, {"msg": "code required for " + base_url + " identity reigstration but not present in query",
                     "status": 400}
      
  return True, {}

def sanitize_remove_identity(request):
  if not "identity_id" in request.args:
    raise ArgError({"code": "invalid_query",
                    "description": "remove-identity endpoint requires identity_id argument"},
                   400)

valid_categories = ["source", "username", "keyword", "url"]
  
def sanitize_blocked_keywords(request):
  if not "word" in request.args:
    raise ArgError({"code": "invalid_query",
                    "description": "blocked-keywords endpoint requires word argument"},
                   400)
  elif len(request.args["word"]) > 32:
    raise ArgError({"code": "invalid_query",
                    "description": "word has a maximum length of 32 characters"},
                   400)    
  if not "category" in request.args:
    raise ArgError({"code": "invalid_query",
                    "description": "blocked-keywords endpoint requires word argument"},
                   400)

  elif not request.args["category"] in valid_categories:
    raise ArgError({"code": "invalid_query",
                    "description": "Invalid category argument"},
                   400)

def sanitize_user_profile(request):
  if not "display_name" in request.args:
    raise ArgError({"code": "invalid_query",
                    "description": "user-profile endpoint requires display_name argument"},
                   400)
  elif len(request.args["display_name"]) > 32:
    raise ArgError({"code": "invalid_query",
                    "description": "display_name has a maximum length of 32 characters"},
                   400)    
      
