import logging
import time
import json
import urllib
from datetime import datetime, timedelta
import httpx
import joy
from .http_error import HTTPError


class GOBOBluesky():
    def __init__(self):
        pass

    # This method block encapsulates how Bluesky tunnels HTTP. It makes the
    # RPC block methods easier to write after bootstrapping is complete.
    def build_url(self, base, query = None):
        url = f"https://bsky.social/xrpc/{base}"
        if query is not None:
            data = {}
            for key, value in query.items():
                if value is not None:
                    data[key] = value
            url += f"?{urllib.parse.urlencode(data)}"
        return url



    # Based on this September 2023 blog post where Bluesky added ratelimits:
    # https://www.docs.bsky.app/blog/rate-limits-pds-v3#adding-rate-limits

    # Bluesky uses a rate limit header negotiation protocol that's part of
    # an IETF standards track proposal:
    # https://www.ietf.org/archive/id/draft-polli-ratelimit-headers-02.html#section-4

    # TODO: This is minimal implementation of the above protocol.
    # Make this more rigorous or switch us over to a published client.
    
    # CAUTION: This seems to be a Unix Epoch time, but the IETF proposal
    #     they link to uses relative seconds. Aside from being an annoying wtf,
    #     they might switch this randomly.
    def get_wait_timeout(self, reset):
        reset = joy.time.convert("unix", "date", reset)
        now = joy.time.nowdate
        return (now - reset).total_seconds() + 1


    def handle_ratelimit(self, url, response):
        remaining = response.headers.get("ratelimit-remaining")
        reset = response.headers.get("ratelimit-reset")

        if remaining is None:
            return
        if int(remaining) > 1:
            logging.info({
                "message": "Bluesky: monitoring ratelimit headers",
                "url": url,
                "remaining": remaining
            })
            return
        
        seconds = self.get_wait_timeout(reset)
        logging.warning(f"Bluesky: proactively slowing to avoid ratelimit. Waiting for {seconds} seconds")
        time.sleep(seconds)
    

    # This is to deal with 429 status responses, but we should never get these
    # if our above rate_limit watcher respects "remaining". It should be the
    # warning we respect before crashing into this violation. That's why
    # these are treated as errors.
    def handle_too_many(self, url, response):
        logging.warning({
            "message": f"Bluesky: 429 response for url {url}",
            "headers": response.headers
        })
        # This shouldn't happen, but we don't have good recourse if it's not here.
        reset = response.headers.get("ratelimit-reset")
        if reset is None:
            raise Exception("Bluesky: got 429 response, but without ratelimit-reset header guidance")

        seconds = self.get_wait_timeout(reset)
        logging.warning(f"Bluesky: got 429 response. Waiting for {seconds} seconds.")
        time.sleep(seconds)


    def handle_response(self, url, r, skip_response = False):
        if r.status_code == 429:
            self.handle_too_many(url, r)
            return {"retry": True}
        
        self.handle_ratelimit(url, r)
        content_type = r.headers.get("content-type")
        body = {}
        if content_type is not None:
            if "application/json" in content_type:
                body = r.json()

        if r.status_code < 400:
            if skip_response == True:
                return {"retry": False, "value": None}
            else:
                return {"retry": False, "value": body}
        else:
            logging.warning(body)
            logging.warning(r.headers)
            raise HTTPError(r.status_code, body, url)
    

    def get(self, url, headers = None, skip_response = False):
        with httpx.Client() as client:
            while True:
                r = client.get(url, headers=headers)
                control = self.handle_response(url, r, skip_response)
                if control["retry"] == False:
                    return control.get("value")


    def post(self, url, data = None, headers = None, skip_response = False):
        with httpx.Client() as client:
            while True:
                r = client.post(url, data=data, headers=headers)
                control = self.handle_response(url, r, skip_response)
                if control["retry"] == False:
                    return control.get("value")
                

    def add_token(self, headers):
        if headers is None:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        else:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers
    
    def handle_data(self, data, headers):
        if data is not None:
            data = json.dumps(data)
            headers["Content-Type"] = "application/json"
        return data
            
      
    def bluesky_get(self, url, headers = None, skip_response = False):
        headers = self.add_token(headers)
        return self.get(url, headers = headers, skip_response=skip_response)     

    def bluesky_post(self, url, data = None, headers = None, skip_response = False):
        headers = self.add_token(headers)
        data = self.handle_data(data, headers)
        return self.post(url, data = data, headers = headers, skip_response=skip_response)



    # This method block deals with the bootstrapping procedure.
    # We also use this block when cycling stale access and refresh tokens.
    def resolve_handle(self, handle):
        url = self.build_url("com.atproto.identity.resolveHandle", {
            "handle": handle
        })

        return self.get(url)

    def create_session(self, did, password):
        url = self.build_url("com.atproto.server.createSession")
        headers = {"Content-Type": "application/json"}
        data = json.dumps({
            "identifier": did,
            "password": password
        })

        return self.post(url, headers = headers, data = data)

    def login(self, login, password):
        data = self.resolve_handle(login)
        did = data["did"]
        logging.info(f"bluesky: createSession for {login}")
        return self.create_session(did, password)
        
    def load_session(self, session):
        self.session = session
        self.did = session["did"]
        self.handle = session["handle"]
        self.access_token = session["access_token"]
        self.refresh_token = session["refresh_token"]

    def refresh_session(self, session):
        url = self.build_url("com.atproto.server.refreshSession")
        refresh_token = session["refresh_token"]
        handle = session["handle"]
        headers = {"Authorization": f"Bearer {refresh_token}"}

        logging.info(f"bluesky: refreshSession for {handle}")
        return self.post(url, headers = headers)


    # Once onboarding is complete, this method block maps GOBO worker actions
    # to the RPC methods specified in the Bluesky lexicon.
    def get_profile(self, actor):
        url = self.build_url("app.bsky.actor.getProfile", {"actor": actor})
        return self.bluesky_get(url)
    
    def get_follows(self, actor, cursor):
        url = self.build_url("app.bsky.graph.getFollows", {
            "actor": actor,
            "cursor": cursor,
            "limit": 100
        })
       
        return self.bluesky_get(url)

    def get_author_feed(self, actor, cursor):
        url = self.build_url("app.bsky.feed.getAuthorFeed", {
            "actor": actor,
            "cursor": cursor,
            "limit": 100
        })
       
        return self.bluesky_get(url)
    
    def get_thread(self, uri, parentHeight = None, depth = None):
        url = self.build_url("app.bsky.feed.getPostThread", {
            "uri": uri,
            "parentHeight": parentHeight,
            "depth": depth,
        })
       
        return self.bluesky_get(url)
    
    def create_post(self, post):
        url = self.build_url("com.atproto.repo.createRecord", {})

        data = {
            "repo": self.did,
            "collection": "app.bsky.feed.post",
            "record": post
        }

        return self.bluesky_post(url, data)
    
    def remove_post(self, rkey):
        url = self.build_url("com.atproto.repo.deleteRecord", {})

        data = {
            "repo": self.did,
            "collection": "app.bsky.feed.post",
            "rkey": rkey
        }

        return self.bluesky_post(url, data)
    
    def get_post(self, reference):
        url = self.build_url("com.atproto.repo.getRecord", {
            "repo": reference.get("did"),
            "collection": "app.bsky.feed.post",
            "rkey": reference.get("rkey")
        })

        return self.bluesky_get(url)
    
    def upload_blob(self, draft):
        url = self.build_url("com.atproto.repo.uploadBlob", {})
        headers = self.add_token(None)
        headers["Content-Type"] = draft["mime_type"]
        data = draft["data"]
        return self.post(url, data = data, headers = headers)
    
    def like_post(self, record):
        url = self.build_url("com.atproto.repo.createRecord", {})

        data = {
            "repo": self.did,
            "collection": "app.bsky.feed.like",
            "record": record
        }

        return self.bluesky_post(url, data)
    
    def undo_like_post(self, rkey):
        url = self.build_url("com.atproto.repo.deleteRecord", {})

        data = {
            "repo": self.did,
            "collection": "app.bsky.feed.like",
            "rkey": rkey
        }

        return self.bluesky_post(url, data, skip_response=True)
    
    def repost_post(self, record):
        url = self.build_url("com.atproto.repo.createRecord", {})

        data = {
            "repo": self.did,
            "collection": "app.bsky.feed.repost",
            "record": record
        }

        return self.bluesky_post(url, data)
    
    def undo_repost_post(self, rkey):
        url = self.build_url("com.atproto.repo.deleteRecord", {})

        data = {
            "repo": self.did,
            "collection": "app.bsky.feed.repost",
            "rkey": rkey
        }

        return self.bluesky_post(url, data, skip_response=True)
    

    def list_notifications(self, cursor, limit = 100):
        url = self.build_url("app.bsky.notification.listNotifications", {
            "cursor": cursor,
            "limit": limit
        })
       
        return self.bluesky_get(url)
    
    def get_posts(self, uris):
        if len(uris) == 0:
            raise Exception("uris list length must be longer than 0")
        if len(uris) > 20:
            raise Exception("uris list length must be 20 or shorter")
        
        def valid_uri(uri):
            return type(uri) is str and uri.startswith("at://")

        values = {}
        index = 0
        for uri in uris:
            if not valid_uri(uri):
                logging.error({"uris": uris})
                raise Exception("value in uris list is not an at-uri string")

            values[f"uris[{index}]"] = uri
            index += 1

        url = self.build_url("app.bsky.feed.getPosts", values)

        return self.bluesky_get(url)