import logging
import json
import urllib
import httpx
import joy


class GOBOBluesky():
    def __init__(self):
        pass


    def get(self, url, headers = None, skip_response = False):
        with httpx.Client() as client:
            r = client.get(url, headers=headers)
            if r.status_code < 400:
                if skip_response == True:
                    return
                else:
                    return r.json()
            else:
                logging.warning(r.json())  
                raise Exception(f"Bluesky: responded with status {r.status_code}")

    def post(self, url, data = None, headers = None, skip_response = False):
        with httpx.Client() as client:
            r = client.post(url, data=data, headers=headers)
            if r.status_code < 400:
                if skip_response == True:
                    return
                else:
                    return r.json()
            else:
                logging.warning(r.json())  
                raise Exception(f"Bluesky: responded with status {r.status_code}")


    def build_url(self, base, query = None):
        url = f"https://bsky.social/xrpc/{base}"
        if query is not None:
            data = {}
            for key, value in query.items():
                if value is not None:
                    data[key] = value
            url += f"?{urllib.parse.urlencode(data)}"
        return url
            
    def add_token(self, headers):
        if headers is None:
            headers = {"Authorization": f"Bearer {self.token}"}
        else:
            headers["Authorization"] = f"Bearer {self.token}"
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


    def resolve_handle(self, handle):
        url = self.build_url("com.atproto.identity.resolveHandle", {
            "handle": handle
        })
     
        return self.get(url)

    def create_session(self, password):
        url = self.build_url("com.atproto.server.createSession")

        return self.post(url, 
            data = json.dumps({
                "identifier": self.did,
                "password": password
            }),
            headers = {
                "Content-Type": "application/json"
            }
        )

    def login(self, login, password):
        data = self.resolve_handle(login)
        self.did = data["did"]
        data = self.create_session(password)
        self.token = data["accessJwt"]


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
    
    def create_post(self, post):
        url = self.build_url("com.atproto.repo.createRecord", {})

        data = {
            "repo": self.did,
            "collection": "app.bsky.feed.post",
            "record": post
        }

        return self.bluesky_post(url, data)
    
    def get_post(self, rkey):
        url = self.build_url("com.atproto.repo.getRecord", {
            "repo": self.did,
            "collection": "app.bsky.feed.post",
            "rkey": rkey
        })

        return self.bluesky_get(url)
    
    def upload_blob(self, draft):
        url = self.build_url("com.atproto.repo.uploadBlob", {})
        headers = self.add_token(None)
        headers["Content-Type"] = f"image/{draft['mime_type']}"
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