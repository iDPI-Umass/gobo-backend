import logging


def dispatch(task):
    task.handler = "test"
    logging.info("dispatching: %s", task)
    task.remove()