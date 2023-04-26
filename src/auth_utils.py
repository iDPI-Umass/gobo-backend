import json
import time
from flask import request, g
from errors import AuthError
from six.moves.urllib.request import urlopen
from authlib.jose import jwt
import pdb

# Adapted from https://auth0.com/docs/quickstart/backend/python/01-authorization
# /server.py


AUTH0_DOMAIN = 'dev-j72vlrggk1ft8e8u.us.auth0.com'
API_AUDIENCE = 'https://gobo.social/api'
ALGORITHM = "RS256"

claims_options = {
  "iss": {
    "essential": True,
    "value": "https://" + AUTH0_DOMAIN + "/"
  },
  "alg": {
    "essential": True,
    "values": ALGORITHM
  },
  "aud": {
    "essential": True,
    "value": API_AUDIENCE
  }
}


# Handle JSON Webtoken Key Set (JWKS) updates from auth0
jwks_timeout = 3600
def update_jwks():
  """Update JSON Web Token Key Set from auth0
  """
  json_url = urlopen("https://"+AUTH0_DOMAIN+"/.well-known/jwks.json")
  g.jwks = json.loads(json_url.read())
  g.jwks_last_update = time.time()

def get_jwks():
  """Get keyset or update if stale
  """
  if 'jwks' not in g or time.time() - g.jwks_last_update >= jwks_timeout:
    update_jwks()
    
  return g.jwks
    
# Format error response and append status code
def get_token_auth_header():
  """Obtains the Access Token from the Authorization Header
  """
  # For protected endpoints, the header must include Authorization.
  # We are using JWT Bearer tokens issued by auth0 (for now)
  # The authorization portion of the header should say bearer followed by the token
  auth = request.headers.get("Authorization", None)
  if not auth:
    raise AuthError({"code": "authorization_header_missing",
                     "description": "Authorization header is expected"},
                    401) 

  parts = auth.split()

  if parts[0].lower() != "bearer":
    raise AuthError({"code": "invalid_header",
                     "description": "Authorization header must start with Bearer"},
                    401)
  elif len(parts) == 1:
    raise AuthError({"code": "invalid_header",
                     "description": "Token not found"},
                    401)
  elif len(parts) > 2:
    raise AuthError({"code": "invalid_header",
                     "description": "Authorization header must be Bearer token"},
                     401)

  token = parts[1]
  return token

def get_token_payload(token):
  try:
    payload = jwt.decode(token, key = get_jwks(), claims_options = claims_options)
    return payload
  except Exception as error:
    print(error)
    raise AuthError({"code": "invalid_header",
                     "description": "Unable to parse authentication token."},
                    401)

def check_permissions(payload, required_permission):
  try:
    if not required_permission in payload["permissions"]:
      raise AuthError({"code": "insufficient_permissions",
                       "description": "User does not have permission: " + required_scope},
                      401)
  except AuthError as error:
    raise error
  except Exception as error:
    print(error)
    raise AuthError({"code": "invalid_headed",
                     "description": "Unable to parse permissions on authentication token."},
                    401)
