import logging
import time
from flask import Flask, request, g
import httpx
from jose import jwt
import http_errors

# TODO: Configuration
AUTH0_DOMAIN = "dev-j72vlrggk1ft8e8u.us.auth0.com"
API_AUDIENCE = "https://gobo.social/api"
ALGORITHMS = ["RS256"]
JWKS_TIMEOUT = 3600

cache = {}


def get_token():
    header = request.headers.get("Authorization")
    if header == None:
        raise http_errors.unauthorized("request is missing authorization header")

    parts = header.split()
    if len(parts) != 2:
        raise http_errors.unauthorized("authorization header does not use expected format")

    if parts[0] != "Bearer":
        raise http_errors.unauthorized("authorization header does not use expected scheme")

    return parts[1]

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
                issuer=f"https://{AUTH0_DOMAIN}/"
            )
        except jwt.ExpiredSignatureError:
            raise http_errors.unauthorized("token is expired")
        except jwt.JWTClaimsError:
            raise http_errors.unauthorized("incorrect claims, please check audience and issuer")
        except Exception:
            raise http_errors.unauthorized("unable to parse authentication token")
    
        return payload
        
    raise http_errors.unauthorized("Unable to find appropriate key")


def get_roles():
    token = get_token()
    claims = validate_token(token)
    g.claims = claims
    roles = claims.get("scope")
    if roles == None:
        return []
    return roles.split()



def authorize_request(configuration):
    schema = configuration.get("request").get("authorization")
    if schema is None:
        schema = []
    
    if "public" in schema:
        return
    
    roles = get_roles()
    if "admin" in roles:
        return
    
    for role in roles:
        if role in schema:
            return

    if "person" in schema:
        person = models.person.lookup(g.claims["sub"])
        g.person = person
        if person["id"] == request.args["id"]:
            return

    
    raise http_errors.unauthorized("requester lacks proper permissions")