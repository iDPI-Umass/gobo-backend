import logging


def dispatch(task):
    task.handler = "mastodon"
    logging.info("dispatching: %s", task)
    task.remove()