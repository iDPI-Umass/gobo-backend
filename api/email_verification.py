import logging
import time
import os
from auth0.authentication import GetToken
from auth0.management import Auth0

DOMAIN = os.environ.get("AUTH0_DOMAIN")
CLIENT_ID = os.environ.get("AUTH0_MANAGEMENT_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AUTH0_MANAGEMENT_CLIENT_SECRET")
# TODO: Revisit this duration.
TIMEOUT = 3600



cache = {}

def is_expired(bundle):
    return time.time() - bundle["time"] >= TIMEOUT

def refresh_client():
    # Fetch access token by authenticating against the Auth0 API with the
    # Machine-to-Machine client secret.
    get_token = GetToken(
        DOMAIN,
        CLIENT_ID, 
        client_secret=CLIENT_SECRET
    )
    url = "https://{}/api/v2/".format(DOMAIN)
    token = get_token.client_credentials(url)

    # Instantiate Management API client using the above access token.
    auth0 = Auth0(DOMAIN, token["access_token"])
    cache["client"] = {
        "value": auth0,
        "time": time.time()
    }

    return auth0



def get_client():
    bundle = cache.get("client")
    if bundle == None:
        return refresh_client()
    elif is_expired(bundle):
        return refresh_client()
    else:
        return bundle["value"]



def resend(person):
    # Get an instance of an Auth0 Management API client.
    client = get_client()
    
    # (Re)send verification email.
    client.jobs.send_verification_email({
        "user_id": person["authority_id"]
    })

    # Above body specified here: https://auth0.com/docs/api/management/v2/jobs/post-verification-email
    # NOTE: We'll need to add the "identity" parameter if we decide to
    #       incorporate social, third-party logins, like Google.