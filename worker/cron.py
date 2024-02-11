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

    # # Regularly updates all identities in Gobo, their feeds, and notifications.
    # # Default background tasking, low priority.
    # schedule.every().hour.do(
    #     queues.default.put_details, 
    #     "fanout update identity", 
    #     {"platform": "all"}
    # )

    # # Prune resources older than our maximum retention timelimit.
    # schedule.every().hours.do(
    #     queues.default.put_details, "prune resources"
    # )