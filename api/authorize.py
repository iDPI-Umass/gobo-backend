import logging
import time
from flask import Flask, request, g
import httpx
from jose import jwt
import http_errors
import models

# TODO: Configuration
AUTH0_DOMAIN = "dev-j72vlrggk1ft8e8u.us.auth0.com"
API_AUDIENCE = "https://gobo.social/api"
ALGORITHMS = ["RS256"]
JWKS_TIMEOUT = 3600

cache = {}


def parse_authorization():
    header = request.headers.get("Authorization")
    if header == None:
        raise http_errors.unauthorized("request is missing authorization header")

    parts = header.split()
    if len(parts) != 2:
        raise http_errors.unauthorized("authorization header does not use expected format")

    return parts

def refresh_keyset():
    with httpx.Client() as client:
        r = client.get(f"https://{AUTH0_DOMAIN}/.well-known/jwks.json")
        value = r.json()

        cache["jwks"] = {
          "value": value,
          "time": time.time()
        }
        return value

def fetch_keyset():
    keyset = cache.get("jwks")
    if keyset == None:      
        return refresh_keyset()
    elif time.time() - keyset["time"] >= JWKS_TIMEOUT:
        return refresh_keyset()
    else:
        return keyset["value"]



def validate_token(token):    
    jwks = fetch_keyset()
    unverified_claims = jwt.get_unverified_header(token)
    
    rsa_key = {}
    for key in jwks["keys"]:
        if key["kid"] == unverified_claims["kid"]:
            rsa_key = {
                "kty": key["kty"],
                "kid": key["kid"],
                "use": key["use"],
                "n": key["n"],
                "e": key["e"]
            }
    
    if rsa_key:
        try:
            payload = jwt.decode(
                token,
                rsa_key,
                algorithms=ALGORITHMS,
                audience=API_AUDIENCE,
                issuer="https://auth.gobo.social/"
            )
        except jwt.ExpiredSignatureError:
            raise http_errors.unauthorized("token is expired")
        except jwt.JWTClaimsError:
            raise http_errors.unauthorized("incorrect claims, please check audience and issuer")
        except Exception:
            raise http_errors.unauthorized("unable to parse authentication token")
    
        return payload
        
    raise http_errors.unauthorized("Unable to find appropriate key")



# This looks up the permissions as maintained by Auth0.
#
# - We need this to verify that a persona has admin access.
# - During the private beta, we also used the permission "general" to provide
#   general per-account level access to regular personas.
#
# As we move into public beta / general access, we're transitioning what
# that "general" permission is.
#
# We're going to look for email verification, which is still a claim signed
# by the authority. Then, we'll add "general" as a virtual permission.
#
# Benefits Of This Approach:
# - This avoids disrupting the authorization flow outside of this function.
# - Pre-existing accounts are gracefully handled.
# - It's semantically consistent with the how the permission "general" is changing.
# - If we wanted the permission to be available for federation, we're not painted
#   into a corner; it's still based on a signed claim. So we have all our
#   options available to us in the future.

def get_permissions(token):
    claims = validate_token(token)
    g.claims = claims

    # Gets the formal permissions from the signed claim.
    permissions = set(claims.get("permissions", []))

    # We honor accounts with verified email addresses as having general access.
    is_verified = claims.get("https://gobo.social/verified", False)
    if is_verified == True:
        permissions.add("general")
    
    return permissions



def lookup_gobo_key(key): 
    key = models.gobo_key.find({"key": key})
    if key is None:
        return None
    
    return key["person_id"]



def authorize_request(configuration):
    try:
        schema = configuration.get("request").get("authorization")
        if schema is None:
            schema = []
        
        if "public" in schema:
            return
        
        # Below this, we're looking at the authorization header.
        parts = parse_authorization()            
        
        
        if parts[0] == "GoboKey":
            # Relies on internally managed bearer credential.
            if "gobo-key" not in schema:
                raise Exception("no matching permissions")

            person_id = lookup_gobo_key(parts[1])
            if person_id is None:
                raise Exception("no matching permissions")
            
            if "general" in schema:
                g.person = models.person.get(person_id)
                return

            if person_id == request.view_args.get("person_id"):
                g.person = models.person.get(person_id)
                return
            
            raise Exception("no matching permissions")
        

        elif parts[0] == "Bearer":
            # Relies on integration with Auth0        
            permissions = get_permissions(parts[1])
        
            if "admin" in permissions:
                return

            if "general" in permissions and "general" in schema:
                return

            if "person" in schema:
                person = models.person.lookup(g.claims["sub"])
                if person["id"] != request.view_args.get("person_id"):
                    raise Exception("no matching permissions")
                g.person = person
                return

            raise Exception("no matching permissions")
 

        else:
            raise http_errors.unauthorized("authorization header does not use expected scheme")
        

    except Exception:
        raise http_errors.unauthorized("requester lacks proper permissions")

    
    