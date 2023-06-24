import logging
import json
from os import environ
import joy
import praw
from .helpers import guess_mime, md

def is_image(url):
    return url.startswith("https://i.redd.it/")

def is_video(url):
    return url.startswith("https://v.redd.it/")

def is_gallery(url):
    return url.startswith("https://www.reddit.com/gallery/")



class Reddit():
    BASE_URL = "https://www.reddit.com"

    def __init__(self, identity = None):
        self.identity = identity or {}
        self.client = praw.Reddit(
            refresh_token = self.identity.get("oauth_token"),
            client_id = environ.get("REDDIT_CLIENT_ID"),
            client_secret = environ.get("REDDIT_CLIENT_SECRET"),
            user_agent = environ.get("REDDIT_USER_AGENT"),
            redirect_uri = environ.get("OAUTH_CALLBACK_URL")
        )


    def get_redirect_url(self, state):
        return self.client.auth.url(
            scopes = ["identity", "mysubreddits", "read"],
            state=state,
            duration = "permanent"
        )

    def convert_code(self, code):
        return self.client.auth.authorize(code)

    def get_profile(self):
        return self.client.user.me()

    def map_sources(self, data):
        base_url = Reddit.BASE_URL
        sources = []
        for subreddit in data["subreddits"]:
            name = str(subreddit)

            sources.append({
                "platform_id": name,
                "base_url": base_url,
                "url": f"{base_url}/r/{name}",
                "username": name,
                "name": name,
                "icon_url":subreddit.icon_img,
                "active": True
            })
  
        return sources


    def map_posts(self, source, data):
        base_url = Reddit.BASE_URL
        
        posts = []
        for submission in data["submissions"]:
            content = None
            attachments = []
            
            if submission.is_self == True:
                content = submission.selftext
            else:
                if is_image(submission.url) == True:
                    attachments.append({
                        "url": submission.url,
                        "type": guess_mime(submission.url)
                    })
                
                elif is_video(submission.url) == True:
                    try:
                        url = submission.media["reddit_video"]["fallback_url"]
                        content_type = guess_mime(url) or "video/mp4"
                        attachments.append({
                            "url": url,
                            "type": content_type
                        })
                    except Exception as e:
                        logging.warning(e)                              
                
                elif is_gallery(submission.url) == True:
                    try:
                        for key, value in submission.media_metadata.items():
                            if value["status"] == "valid":
                                content_type = value["m"]
                                best = None
                                best_area = 0
                                for entry in value["p"]:
                                    entry_area = entry["x"] * entry["y"]
                                    if best == None or entry_area > best_area:
                                        best = entry
                                        best_area = entry_area
                                 
                                attachments.append({
                                    "url": best["u"],
                                    "type": content_type
                                })
                    except Exception as e:
                        logging.warning(e) 
            

            post = {
                "source_id": source["id"],
                "base_url": base_url,
                "platform_id": submission.id,
                "title": submission.title,
                "content": content,
                "url": base_url + submission.permalink,
                "published": joy.time.unix_to_iso(submission.created_utc),
                "attachments": attachments
            }

            posts.append(post)

        return posts


    def list_sources(self):
        generator = self.client.user.subreddits(limit=None)
        subreddits = list(generator)
        return {"subreddits": subreddits} 


    def list_posts(self, source):
        submissions = []
        name = source["platform_id"]
        last_retrieved = None
        # last_retrieved = source.get("last_retrieved")
        generator = self.client.subreddit(name).new(limit=None)


        if last_retrieved == None:
            total = 1
            for submission in generator:
                submissions.append(submission)
                total = total + 1
                if total > 100:
                    break
        else:
            for submission in generator:
                timestamp = joy.time.unix_to_iso(submission.created_utc)
                if timestamp > last_retrieved:
                    submissions.append(submission)
                else:
                    break
        
        return {
            "submissions": submissions,
            "subreddits": []
        }