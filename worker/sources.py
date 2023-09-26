import logging
import time
import schedule
import models
import queues


# Make sure we wait until we have a stable connection to the database.
def safe_start():
    while True:
        try:
            result = models.task.get(1)
            break
        except Exception as e:
            time.sleep(1)


def start_sources():
    safe_start()
    queues.api.put_details("poll", {})

    # Standalone tasks for Bluesky
    queues.default.put_details("bluesky cycle sessions")
    schedule.every(20).minutes.do(
        queues.default.put_details, "bluesky cycle sessions"
    )
    
    # Manage image upload cache
    queues.default.put_details("bluesky cycle sessions")
    schedule.every(12).hours.do(
        queues.default.put_details, "prune image cache"
    )

    # # Handles follower-list (source) updates.
    # schedule.every(12).hours.do(
    #     queues.default.put_details, "pull sources fanout"
    # )

    # # Pull the latest posts from the tracked sources.
    # schedule.every().hour.do(
    #     queues.bluesky.put_details, "read sources"
    # )

    # schedule.every().hour.do(
    #     queues.reddit.put_details, "read sources"
    # )

    # schedule.every().hour.do(
    #     queues.mastodon.put_details, "read sources"
    # )