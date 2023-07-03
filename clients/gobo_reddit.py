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
            body = r.json()
            for post in body["data"]["children"]:
                output.append(post["data"])

        return output
        