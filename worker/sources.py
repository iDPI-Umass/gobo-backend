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

    # schedule.every(6).hours.do(
    #     queues.twitter.put, queues.Task("identity follow fanout")
    # )

    # schedule.every(6).hours.do(
    #     queues.reddit.put, queues.Task("identity follow fanout")
    # )

    # schedule.every(6).hours.do(
    #     queues.mastodon.put, queues.Task("identity follow fanout")
    # )