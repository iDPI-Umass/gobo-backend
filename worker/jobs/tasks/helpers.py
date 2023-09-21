from clients import Bluesky
import models

where = models.helpers.where


supported_platforms = [
  "all",
  "bluesky",
  "mastodon",
  "reddit",
  "smalltown"
]

def is_valid_platform(platform):
  return platform in supported_platforms

def add_bluesky(ax = []):
    ax.append(where("base"))