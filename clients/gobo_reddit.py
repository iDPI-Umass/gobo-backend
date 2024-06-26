import logging
import os
import time
import json
import httpx
import joy


class HTTPError(Exception):
  def __init__(self, status, response):
      self.status = status
      self.response = response

# TODO: Do something more sophisticated here. It appears that Reddit's rate limit
#   header protocol sends numbers with floating point precision and python
#   is precise about such things. 
def to_number(string):
    try:
        result = float(string)
        return result
    except ValueError:
        return int(string)


class GOBOReddit():
    def __init__(self):
        pass


    # These methods observe Reddit's set of ratelimit headers that give hints
    # about pacing on HTTP requests. Based on these three:
    # ('x-ratelimit-remaining', '95')
    # ('x-ratelimit-used', '1')
    # ('x-ratelimit-reset', '503')
       
    def get_wait_timeout(self, reset):
        if reset is None:
            logging.warning("Reddit: x-ratelimit-reset header is not available")
            reset = 500
            
        return to_number(reset) + 2

    def handle_ratelimit(self, url, response):
        remaining = response.headers.get("x-ratelimit-remaining")
        reset = response.headers.get("x-ratelimit-reset")

        if remaining is None:
            return
        if to_number(remaining) > 2:
            logging.info({
                "message": "Reddit: monitoring ratelimit headers",
                "url": url,
                "remaining": remaining,
                "used": response.headers.get("x-ratelimit-used")
            })
            return
        
        seconds = self.get_wait_timeout(reset)
        logging.warning({
            "message": f"Reddit: proactively slowing to avoid ratelimit. Waiting for {seconds} seconds",
            "timeout": seconds
        })
        time.sleep(seconds)

    def handle_too_many(self, url, response):
        timeout = response.headers.get("x-ratelimit-reset", 500)
        timeout = to_number(timeout) + 1
        logging.warning({
            "message": f"Reddit: Got 429 response. Backing off for {timeout} seconds",
            "url": url,
            "timeout": timeout
        })
        time.sleep(timeout)

    def handle_error(self, url, error):
        logging.warning({
            "url": url,
            "status": error.status,
            "headers": error.response.headers,
            "body": self.get_body(error.response),
        })


    def get_body(self, response):
        content_type = response.headers.get("content-type")
        body = {}
        if content_type is not None:
            if "application/json" in content_type:
                body = response.json()
            else:
                logging.warning("Reddit: got non JSON response")
        return body
  

    def issue_request(self, method, url, headers):
        with httpx.Client() as client:
            while True:
                request = getattr(client, method)
                response = request(url, headers = headers)
                
                if response.status_code == 200:
                    self.handle_ratelimit(url, response)
                    return response
                elif response.status_code == 429:
                    self.handle_too_many(url, response)
                else:
                    raise HTTPError(response.status_code, response)



    def get_new_ids(self, subreddit):
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"
        headers = {
            "User-Agent": os.environ.get("REDDIT_USER_AGENT")
        }

        try:
            response = self.issue_request("get", url, headers)
        except HTTPError as e:
            # Re-raise special case for source lockout.
            if e.status == 403:
                raise e
            self.handle_error(url, e)
            return []
        
        body = self.get_body(response)
        data = body.get("data")
        if data is None:
            logging.warning(f"Reddit: fetching posts for {subreddit} but response did not include data")
            logging.warning(body)
            return []
       
        output = []
        for post in data["children"]:
            output.append(post["data"])
        return output