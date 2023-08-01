import logging
import json
import httpx
import joy


class GOBOReddit():
    def __init__(self):
        pass

    def get_new_ids(self, subreddit):
        output = []

        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"

        with httpx.Client() as client:
            r = client.get(url)
            if r.status_code == 200:
                body = r.json()
                if body.get("data", None) is None:
                    logging.warning(f"Reddit: Fetching posts for {subreddit} but response did not include data")
                    logging.warning(body)
                else:
                    for post in body["data"]["children"]:
                        output.append(post["data"])
            else:
                logging.warning(f"Reddit: fetching posts for {subreddit} responded with status {r.status_code}")
                logging.warning(r.json())

        return output
        