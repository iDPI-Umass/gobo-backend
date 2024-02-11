import logging
import os
import time
import json
import httpx
import joy


class GOBOReddit():
    def __init__(self):
        pass

    def get_new_ids(self, subreddit):
        output = []

        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"
        headers = {
            "User-Agent": os.environ.get("REDDIT_USER_AGENT")
        }

        with httpx.Client() as client:
            while True:
                r = client.get(url, headers = headers)
                if r.status_code == 200:
                    body = r.json()
                    if body.get("data", None) is None:
                        logging.warning(f"Reddit: Fetching posts for {subreddit} but response did not include data")
                        logging.warning(body)
                    else:
                        for post in body["data"]["children"]:
                            output.append(post["data"])
                    return output
                
                elif r.status_code == 429:
                    raw_timeout = r.headers.get("X-Ratelimit-Reset")
                    logging.info(f"Reddit: Ratelimited reached with directive {raw_timeout}")
                    timeout = r.headers.get("X-Ratelimit-Reset", 3)
                    timeout = int(timeout) + 1
                    logging.info(f"Reddit: Ratelimit reachhed. Backing off for {timeout} seconds.")
                    time.sleep(timeout)
                    continue
                
                else:
                    logging.warning(f"Reddit: fetching posts for {subreddit} responded with status {r.status_code}")
                    logging.warning(r.json())
                    logging.warning(r.headers)
                    return output