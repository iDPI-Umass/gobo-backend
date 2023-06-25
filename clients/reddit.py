import logging
import json
from os import environ
import praw
import joy
import models
from .helpers import guess_mime, md, partition

def is_image(url):
    return url.startswith("https://i.redd.it/")

def is_video(url):
    return url.startswith("https://v.redd.it/")

def is_gallery(url):
    return url.startswith("https://www.reddit.com/gallery/")

class Submission():
    def __init__(self, _):
        logging.info({
            "id": _.name,
            "title": _.title
        })
        self._ = _
        self.id = _.name
        self.title = _.title
        self.content = None
        self.published = joy.time.unix_to_iso(_.created_utc)
        self.url = Reddit.BASE_URL + _.permalink
        self.subreddit = Subreddit(_.subreddit)
        self.crosspost_parent = getattr(_, "crosspost_parent", None)
        self.poll = getattr(_, "poll_data", None)
        self.attachments = []

        if is_image(_.url) == True:
            self.attachments.append({
                "url": _.url,
                "type": guess_mime(_.url)
            })
        
        elif is_video(_.url) == True:
            try:
                url = _.media["reddit_video"]["fallback_url"]
                content_type = guess_mime(url) or "video/mp4"
                self.attachments.append({
                    "url": url,
                    "type": content_type
                })
            except Exception as e:
                logging.warning(e)                              
        
        elif is_gallery(_.url) == True:
            try:
                for key, value in _.media_metadata.items():
                    if value["status"] == "valid":
                        content_type = value["m"]
                        best = None
                        best_area = 0
                        for entry in value["p"]:
                            entry_area = entry["x"] * entry["y"]
                            if best == None or entry_area > best_area:
                                best = entry
                                best_area = entry_area
                          
                        self.attachments.append({
                            "url": best["u"],
                            "type": content_type
                        })
            except Exception as e:
                logging.warning(e)

        elif self.poll != None:
            pass
        elif _.is_self == True:
            self.content = getattr(_, "selftext", None)
        else:
            self.content = _.url

# The id and username swap is awkward. The subreddit "name" is its full name,
# an absolute reference in the Reddit API. However, we reference subreddits
# in the praw client method by using their display_name.
class Subreddit():
    def __init__(self, _):
        self._ = _
        self.id = _.name
        self.url = f"{Reddit.BASE_URL}/r/{_.display_name}",
        self.username = _.id
        self.name = _.display_name
        self.icon_url = _.icon_img




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

    def get_post(self, id):
        submission = self.client.submission(id = id)
        return submission

    def pluck_posts(self, ids):
        generator = self.client.info(ids)
        submissions = []
        for item in generator:
            submissions.append(Submission(item))
        return submissions

    def map_sources(self, data):
        subreddits = data.get("subreddits") or []

        sources = []
        for subreddit in subreddits:
            sources.append({
                "platform_id": subreddit.id,
                "base_url": Reddit.BASE_URL,
                "url": subreddit.url,
                "username": subreddit.username,
                "name": subreddit.name,
                "icon_url":subreddit.icon_url,
                "active": True
            })
  
        return sources


    def map_posts(self, data):
        sources = {}
        for item in data["sources"]:
            sources[item["platform_id"]] = item

        posts = []
        edges = []
        for submission in data["submissions"]:
            source = sources[submission.subreddit.id]

            posts.append({
                "source_id": source["id"],
                "base_url": Reddit.BASE_URL,
                "platform_id": submission.id,
                "title": submission.title,
                "content": submission.content,
                "url": submission.url,
                "published": submission.published,
                "attachments": submission.attachments
            })

            if submission.crosspost_parent != None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": submission.id,
                    "target_type": "post",
                    "target_reference": submission.crosspost_parent,
                    "name": "shares",
                })

        return {
            "posts": posts,
            "edges": edges
        }


    def list_sources(self):
        generator = self.client.user.subreddits(limit=None)
        subreddits = []
        for item in generator:
            subreddits.append(Subreddit(item))
        return {"subreddits": subreddits} 


    def get_post_graph(self, source):
        submissions = []
        subreddits = []

        name = source["name"]
        last_retrieved = source.get("last_retrieved")
        generator = self.client.subreddit(name).new(limit=None)



        if last_retrieved == None:
            total = 1
            for item in generator:
                submissions.append(Submission(item))
                total = total + 1
                if total > 25:
                    break
        else:
            for item in generator:
                timestamp = joy.time.unix_to_iso(item.created_utc)
                if timestamp > last_retrieved:
                    submissions.append(Submission(item))
                else:
                    break
        


        secondary = set()
        for submission in submissions:
            if submission.crosspost_parent != None:
                secondary.add(submission.crosspost_parent)

        registered = models.post.pull([
            models.helpers.where("base_url", Reddit.BASE_URL),
            models.helpers.where("platform_id", list(secondary), "in")
        ])

        for item in registered:
            secondary.remove(item["platform_id"])

        if len(secondary) > 0:
            for sublist in list(partition(list(secondary), 100)):
                generator = self.client.info(sublist)
                for item in generator:
                    submissions.append(Submission(item))



        seen_subreddits = set()
        for submission in submissions:
            if submission.subreddit.id not in seen_subreddits:
                seen_subreddits.add(submission.subreddit.id)
                subreddits.append(submission.subreddit)



        return {
            "submissions": submissions,
            "subreddits": subreddits
        }