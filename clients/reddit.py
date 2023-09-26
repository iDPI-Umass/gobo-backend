import logging
import time
from os import environ
from datetime import timedelta
import json
from os import environ
import html
import praw
from pmaw import PushshiftAPI
import joy
import models
from .gobo_reddit import GOBOReddit
from .helpers import guess_mime, md, partition

gobo_reddit = GOBOReddit()

def is_image(url):
    return url.startswith("https://i.redd.it/")

def is_video(url):
    return url.startswith("https://v.redd.it/")

def is_gallery(url):
    return url.startswith("https://www.reddit.com/gallery/")

def get_subreddit(submission):
    subreddit = submission["subreddit"]
    if type(subreddit) is str:
        return subreddit
    else:
        return subreddit.display_name

def get_poll(submission):
    poll = submission.get("poll_data", None)
    if poll is not None:
        return poll
    
    poll = getattr(submission, "poll_data", None)
    if poll is not None:
        output = vars(poll)
        output.options = []
        for option in poll.options:
            output.append(vars(option))
        return output

    return None

def build_submission(item):
    try:
        return Submission(item)
    except Exception as e:
        logging.error(e, exc_info=True)
        logging.error("\n\n")
        logging.error(item)
        logging.error("\n\n")
        return None

class Submission():
    def __init__(self, _):        
        self._ = _
        self.id = f"t3_{_['id']}"
        self.title = _.get("title", None)
        self.content = None
        self.published = joy.time.convert(
            start = "unix",
            end = "iso",
            value = _["created_utc"],
        )
        self.url = Reddit.BASE_URL + _["permalink"]
        self.subreddit = get_subreddit(_)
        self.crosspost_parent = _.get("crosspost_parent", None)
        self.attachments = []
        self.poll = None

        if self.title is not None:
            self.title = html.unescape(self.title)

        url = _["url"]
        poll = get_poll(_)

        if is_image(url) == True:
            self.attachments.append({
                "url": url,
                "type": guess_mime(url)
            })
        
        elif is_video(url) == True:
            try:
                url = _["media"]["reddit_video"]["fallback_url"]
                content_type = guess_mime(url) or "video/mp4"
                self.attachments.append({
                    "url": url,
                    "type": content_type
                })
            except Exception as e:
                logging.warning(e)                              
        
        elif is_gallery(url) == True:
            try:
                for key, value in _["media_metadata"].items():
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

        elif poll is not None:
            ends = poll["voting_end_timestamp"]
            if ends > 1e10:
                ends = ends / 1000

            self.poll = {
                "total": poll["total_vote_count"],
                "ends": joy.time.convert(
                    start = "unix",
                    end = "iso",
                    value = ends,
                    optional = True
                ),
                "options": []
            }
       
            for option in poll["options"]:
                self.poll["options"].append({
                    "key": option.get("text", ""),
                    "count": option.get("vote_count", 0)
                })

        elif _["is_self"] == True:
            self.content = _.get("selftext", None)
        else:
            self.content = url

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
        self.identity = identity

    @staticmethod
    def get_redirect_url(state):
        client = praw.Reddit(
            client_id = environ.get("REDDIT_CLIENT_ID"),
            client_secret = environ.get("REDDIT_CLIENT_SECRET"),
            user_agent = environ.get("REDDIT_USER_AGENT"),
            redirect_uri = environ.get("OAUTH_CALLBACK_URL")
        )

        return client.auth.url(
            scopes = ["identity", "mysubreddits", "read", "submit", "vote"],
            state=state,
            duration = "permanent"
        )
    
    @staticmethod
    def convert_code(code):
        client = praw.Reddit(
            client_id = environ.get("REDDIT_CLIENT_ID"),
            client_secret = environ.get("REDDIT_CLIENT_SECRET"),
            user_agent = environ.get("REDDIT_USER_AGENT"),
            redirect_uri = environ.get("OAUTH_CALLBACK_URL")
        )
        return client.auth.authorize(code)

    def login(self):
        self.client = praw.Reddit(
            refresh_token = self.identity.get("oauth_token"),
            client_id = environ.get("REDDIT_CLIENT_ID"),
            client_secret = environ.get("REDDIT_CLIENT_SECRET"),
            user_agent = environ.get("REDDIT_USER_AGENT"),
            redirect_uri = environ.get("OAUTH_CALLBACK_URL")
        )

    def convert_code(self, code):
        return self.client.auth.authorize(code)

    def get_profile(self):
        return self.client.user.me()

    def get_post(self, id):
        item = self.client.submission(id = id)
        submission = Submission(item)
        return submission

    def pluck_posts(self, ids):
        generator = self.client.info(ids)
        submissions = []
        for item in generator:
            submissions.append(Submission(item))
        return submissions
    
    def create_post(self, post, metadata):
        title = post.get("title", None)
        if title is None:
            raise Exception("reddit posts must include a title")
        subreddit = metadata.get("subreddit", None)
        if subreddit is None:
            raise Exception("reddit post requires a subreddit to be specified in metadata")
        

        images = []
        for attachment in post.get("attachments", []):
            images.append({
                "image_path": attachment["image_path"],
                "caption": attachment["alt"]
            })        

        if len(images) == 0:
            self.client.subreddit(subreddit).submit(
                title = title,
                selftext = post.get("content", None),
                nsfw = metadata.get("nsfw", False),
                spoiler = metadata.get("spoiler", False)
            )
        elif len(images) == 1:
            self.client.subreddit(subreddit).submit_image(
                title = title,
                image_path = images[0]["image_path"],
                nsfw = metadata.get("nsfw", False),
                spoiler = metadata.get("spoiler", False),
                without_websockets = True
            )         
        else:
            # TODO: How do we want to handle post content for galleries?
            self.client.subreddit(subreddit).submit_gallery(
                title = title,
                images = images,
                nsfw = metadata.get("nsfw", False),
                spoiler = metadata.get("spoiler", False)
            )
        time.sleep(1)

    def create_reply(self, post, metadata):
        reply = metadata["reply"]

        return self.client.submission(reply["platform_id"]).reply(
            body = post["content"]
        )


    def upvote_post(self, post):
        self.client.submission(post["platform_id"]).upvote()
        time.sleep(1)

    def downvote_post(self, post):
        self.client.submission(post["platform_id"]).downvote()
        time.sleep(1)

    def undo_vote_post(self, post):
        self.client.submission(post["platform_id"]).clear_vote()
        time.sleep(1)



    def map_sources(self, data):
        subreddits = data.get("subreddits") or []

        sources = []
        for subreddit in subreddits:
            sources.append({
                "platform": "reddit",
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
        partials = []
        edges = []
        for submission in data["submissions"]:
            if submission.id is None:
                continue

            source = sources[submission.subreddit.id]

            posts.append({
                "source_id": source["id"],
                "base_url": Reddit.BASE_URL,
                "platform": "reddit",
                "platform_id": submission.id,
                "title": submission.title,
                "content": submission.content,
                "url": submission.url,
                "published": submission.published,
                "attachments": submission.attachments,
                "poll": submission.poll
            })

            if submission.crosspost_parent != None:
                edges.append({
                    "origin_type": "post",
                    "origin_reference": submission.id,
                    "target_type": "post",
                    "target_reference": submission.crosspost_parent,
                    "name": "shares",
                })

        for submission in data["partials"]:
            if submission.id is None:
              continue

            source = sources[submission.subreddit.id]

            partials.append({
                "source_id": source["id"],
                "base_url": Reddit.BASE_URL,
                "platform": "reddit",
                "platform_id": submission.id,
                "title": submission.title,
                "content": submission.content,
                "url": submission.url,
                "published": submission.published,
                "attachments": submission.attachments,
                "poll": submission.poll
            })


        return {
            "posts": posts,
            "partials": partials,
            "edges": edges
        }


    def list_sources(self):
        generator = self.client.user.subreddits(limit=None)
        time.sleep(1)
        subreddits = []
        for item in generator:
            subreddits.append(Subreddit(item))
        return {"subreddits": subreddits} 


    def get_post_graph(self, source, last_retrieved = None, is_shallow = False):
        submissions = []
        partials = []
        subreddits = []

        oldest_limit = joy.time.convert("date", "iso", 
            joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
        )

        name = source["name"]
        _submissions = []
        for item in gobo_reddit.get_new_ids(name):
            submission = build_submission(item)
            if submission is not None:
                _submissions.append(submission)

        time.sleep(0.5)

        if last_retrieved is None:
            for submission in _submissions:
                if submission.published < oldest_limit:
                    continue
                submissions.append(submission)
        else:
            for submission in _submissions:
                if submission.published < oldest_limit:
                    continue
                if submission.published > last_retrieved:
                    submissions.append(submission)



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
                generator = self.client.info(fullnames = sublist)
                time.sleep(1)
                for item in generator:
                    partials.append(Submission(vars(item)))



        seen_subreddits = set()
        subreddit_dict = {}
        for submission in submissions:
            if submission.subreddit not in seen_subreddits:
                seen_subreddits.add(submission.subreddit)
        for submission in partials:
            if submission.subreddit not in seen_subreddits:
                seen_subreddits.add(submission.subreddit)

        generator = self.client.info(subreddits = list(seen_subreddits))
        time.sleep(1)
        for item in generator:
            subreddit = Subreddit(item)
            subreddits.append(subreddit)
            subreddit_dict[subreddit.name] = subreddit

        for submission in submissions:
            submission.subreddit = subreddit_dict[submission.subreddit]
        for submission in partials:
            submission.subreddit = subreddit_dict[submission.subreddit]


        return {
            "submissions": submissions,
            "partials": partials,
            "subreddits": subreddits
        }