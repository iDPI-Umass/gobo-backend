import logging
import os
import json
import urllib
import httpx
from .http_error import HTTPError
import clients.helpers as h


class GoboLinkedin():
    BASE_URL = "https://www.linkedin.com"

    def __init__(self):
        pass

    @staticmethod
    def make_login_url(context):
        data = {
            "response_type": "code",
            "client_id": os.environ.get("LINKEDIN_CLIENT_ID"),
            "redirect_uri": os.environ.get("OAUTH_CALLBACK_URL"),
            "scope": context["scope"],
            "state": context["state"]
        }

        url = "https://www.linkedin.com/oauth/v2/authorization?" + \
            urllib.parse.urlencode(data)
    
        return url
    
    @staticmethod
    def exchange_code(code):
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        
        data = urllib.parse.urlencode({
            "grant_type": "authorization_code",
            "code": code,
            "client_id": os.environ.get("LINKEDIN_CLIENT_ID"),
            "client_secret": os.environ.get("LINKEDIN_CLIENT_SECRET"),
            "redirect_uri": os.environ.get("OAUTH_CALLBACK_URL"),
        })

        headers = {
            "content-type": "application/x-www-form-urlencoded",
            "accept": "application/json"
        }
        
        with httpx.Client() as client:
            response = client.post(url, data=data, headers=headers)
            if response.status_code != 200:
                logging.warning(h.get_body(response))
                raise Exception("non-200 response for code exchange request")
            
            return response.json()
        
    @staticmethod
    def get_userinfo(token):
        url = "https://api.linkedin.com/v2/userinfo"

        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {token}"
        }
        
        with httpx.Client() as client:
            response = client.get(url, headers=headers)
            if response.status_code != 200:
                body = h.get_body(response)
                logging.warning(body)
                raise HTTPError(response.status_code, body, url)
            
            return response.json()


    # After bootstrapping is complete, this block makes HTTP interactions easier.
    def build_url(self, resource, query = None):
        url = f"https://api.linkedin.com/v2/{resource}"
        if query is not None:
            data = {}
            for key, value in query.items():
                if value is not None:
                    data[key] = value
            url += f"?{urllib.parse.urlencode(data)}"
        return url
    
    # TODO: put something more sophisticated here. Need to consider how to
    #   approach retries and rate-limiting headers.
    def monitor(self, url, response):
        logging.info({
            "message": "LinkedIn: monitoring request cycle",
            "url": url,
            "status": response.status_code,
            "headers": response.headers
        })
    

    def handle_response(self, url, response):
        self.monitor(url, response)

        if response.status_code < 400:
            return response
        else:
            body = h.get_body(response)
            logging.warning(body)
            logging.warning(response.headers)
            raise HTTPError(response.status_code, body, url)
    
    def get(self, url, headers = None, skip_response = False):
          with httpx.Client() as client:
              response = client.get(url, headers=headers)
              return self.handle_response(url, response)

    def post(self, url, data = None, headers = None, skip_response = False):
        with httpx.Client() as client:
            response = client.post(url, data=data, headers=headers)
            return self.handle_response(url, response)
  

    def add_token(self, headers = None):
        if headers is None:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        else:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers
    
    def add_restli(self, headers = None):
        if headers is None:
            headers = {"X-Restli-Protocol-Version": "2.0.0"}
        else:
            headers["X-Restli-Protocol-Version"] = "2.0.0"
        return headers

    def handle_data(self, data, headers):
        if data is not None:
            data = json.dumps(data)
            headers["Content-Type"] = "application/json"
        return data
            

    def linkedin_get(self, url, headers = None, skip_response = False):
        headers = self.add_token(headers)
        self.add_restli(headers)
        return self.get(url, headers = headers, skip_response=skip_response)     

    def linkedin_post(self, url, data = None, headers = None, skip_response = False):
        headers = self.add_token(headers)
        self.add_restli(headers)
        data = self.handle_data(data, headers)
        return self.post(url, data = data, headers = headers, skip_response=skip_response)



    # Finally, the actual interface we'd like to expose publicly.
    def login(self, token):
        self.access_token = token

    def create_upload_slot(self, urn):
        url = "https://api.linkedin.com/v2/assets?action=registerUpload"

        data = {
            "registerUploadRequest": {
                "recipes": [
                    "urn:li:digitalmediaRecipe:feedshare-image"
                ],
                "owner": urn,
                "serviceRelationships": [
                    {
                        "relationshipType": "OWNER",
                        "identifier": "urn:li:userGeneratedContent"
                    }
                ]
            }
        }

        response = self.linkedin_post(url, data)
        return response.json()

    def upload_media(self, url, draft):
        headers = self.add_token()
        headers["Content-Type"] = f"image/{draft['mime_type']}"
        data = draft["data"]
        return self.post(url, data = data, headers = headers)

    def create_post(self, data):
        url = self.build_url("ugcPosts")
        response = self.linkedin_post(url, data)
        return response.json()
        