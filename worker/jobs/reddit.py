import logging


def dispatch(task):
    task.handler = "reddit"
    logging.info("dispatching: %s", task)
    task.remove()