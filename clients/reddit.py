import logging
import time
from os import environ
from datetime import timedelta
import json
from os import environ
import re
import html
import praw
from pmaw import PushshiftAPI
import joy
import models
from .gobo_reddit import GOBOReddit, HTTPError
import clients.helpers as h

gobo_reddit = GOBOReddit()

def is_image(url):
    return url.startswith("https://i.redd.it/")

def is_video(submission):
    url = submission.get("url")
    media = submission.get("media")
    return url is not None and \
        url.startswith("https://v.redd.it/") and \
        media is not None

def is_gallery(submission):
    url = submission.get("url")
    metadata = submission.get("media_metadata")
    return url is not None and \
        url.startswith("https://www.reddit.com/gallery/") and \
        metadata is not None

# This is purely tactical. The JSON that comes back for galleries in the "new" listing
# contains "preview" URLs that are producing 403 responses. We see that "i" URLs
# do not produce those responses. For now, we just map it emperically.
def correct_media_url(url):
    return re.sub(r"https://preview.redd.it", "https://i.redd.it", url)

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
                "type": h.guess_mime(url)
            })
        
        elif is_video(_) == True:
            try:
                url = _["media"]["reddit_video"]["fallback_url"]
                content_type = h.guess_mime(url) or "video/mp4"
                self.attachments.append({
                    "url": url,
                    "type": content_type
                })
            except Exception as e:
                logging.warning(_)
                logging.warning(e, exc_info=True)                              
        
        elif is_gallery(_) == True:
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
                            "url": correct_media_url(best["u"]),
                            "type": content_type
                        })
            
            except Exception as e:
                logging.warning(_)
                logging.warning(e, exc_info=True)

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
        self.icon_url = _.community_icon



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
            # TODO: It's bad that we hardcode this. Our Reddit integration
            # currently only serves the production redirect. We should see
            # if we can submit multiple values or setup multiple applications.
            redirect_uri = "https://gobo.social/add-identity-callback"
            # redirect_uri = environ.get("OAUTH_CALLBACK_URL")
        )

        return client.auth.url(
            scopes = ["identity", "mysubreddits", "read", "submit", "vote", "edit"],
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

    def get_profile(self):
        return self.client.user.me()

    def map_profile(self, data):
        profile = data["profile"]
        identity = data["identity"]

        identity["profile_url"] = f"{self.BASE_URL}/user/{profile.name}"
        identity["profile_image"] = profile.icon_img,
        identity["username"] = profile.name
        identity["name"] = profile.name
        return identity


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
        

        attachments = post.get("attachments", [])

        if len(attachments) == 0:
            result = self.client.subreddit(subreddit).submit(
                title = title,
                selftext = post.get("content", None),
                nsfw = metadata.get("nsfw", False),
                spoiler = metadata.get("spoiler", False)
            )
        
        elif len(attachments) == 1:
            type = attachments[0]["mime_type"]
            if type.startswith("image"):
                result = self.client.subreddit(subreddit).submit_image(
                    title = title,
                    image_path = attachments[0]["image_path"],
                    nsfw = metadata.get("nsfw", False),
                    spoiler = metadata.get("spoiler", False),
                    without_websockets = True
                )
            elif type.startswith("video"):
                result = self.client.subreddit(subreddit).submit_video(
                    title = title,
                    video_path = attachments[0]["image_path"],
                    nsfw = metadata.get("nsfw", False),
                    spoiler = metadata.get("spoiler", False),
                    without_websockets = True
                )
        
        else:
            # TODO: How do we want to handle post content for galleries?
            files = []
            for attachment in attachments:
                files.append({
                    "image_path": attachment["image_path"],
                    "caption": attachment.get("alt", "")
                })  

            result = self.client.subreddit(subreddit).submit_gallery(
                title = title,
                images = files,
                nsfw = metadata.get("nsfw", False),
                spoiler = metadata.get("spoiler", False)
            )

        # The result is a submission instance, but unless I'm inspecting it
        # incorrectly, it doesn't seem to have very many attributes.
        raw_id = str(result)

        output = {}
        output["id"] = f"t3_{raw_id}"
        
        url = Reddit.BASE_URL
        url += "/r/"
        url += subreddit
        url += "/comments/"
        url += raw_id
        output["url"] = url

        return output

    
    def remove_post(self, reference):
        return self.client.submission(reference).delete()


    def create_reply(self, post, metadata):
        reply = metadata["reply"]

        return self.client.submission(reply["platform_id"]).reply(
            body = post["content"]
        )


    def upvote_post(self, post):
        self.client.submission(post["platform_id"]).upvote()

    def downvote_post(self, post):
        self.client.submission(post["platform_id"]).downvote()

    def undo_vote_post(self, post):
        self.client.submission(post["platform_id"]).clear_vote()


    # We need to discuss how to handle notifications from Reddit, so for now,
    # this will be a passthrough no-op that hooks into the outer notification flow.
    def list_notifications(self, data):
        return {
            "submissions": [],
            "partials": [],
            "subreddits": [],
            "notifications": []
        }
    
    def dismiss_notification(self, notification):
        pass


    def map_notifications(self, data):
        return []




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
    

    def lockout_source(self, source):
        handle = models.link.Lockout("source", source["id"], "source-lockout")
        handle.lock()
        logging.warning(f"Reddit: subreddit r/{source['name']} is not public. Gobo worker will lock this source out for now.")


    def map_posts(self, data):
        preexisting = data.get("preexisting", [])
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

            if submission.crosspost_parent is not None:
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
            "preexisting": preexisting,
            "edges": edges,
        }


    def list_sources(self):
        generator = self.client.user.subreddits(limit=None)
        subreddits = []
        for item in generator:
            subreddits.append(Subreddit(item))
        return {"subreddits": subreddits} 


    def get_post_graph(self, source, last_retrieved = None, is_shallow = False):
        submissions = []
        partials = []
        preexisting = []
        subreddits = []

        oldest_limit = joy.time.convert("date", "iso", 
            joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
        )

        name = source["name"]
        _submissions = []
        logging.info(f"Reddit: Fetch r/{source['name']}")

        try:
            items = gobo_reddit.get_new_ids(name)
        # Special case for source lockouts.
        except HTTPError:
            self.lockout_source(source)
            return False
        
        for item in items:
            submission = build_submission(item)
            if submission is not None:
                _submissions.append(submission)

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
            preexisting.append(item)
            secondary.remove(item["platform_id"])

        if len(secondary) > 0:
            for sublist in list(h.partition(list(secondary), 100)):
                generator = self.client.info(fullnames = sublist)
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
            "preexisting": preexisting,
            "subreddits": subreddits
        }