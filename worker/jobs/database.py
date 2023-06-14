import logging


def dispatch(task):
    task.handler = "database"
    logging.info("dispatching: %s", task)
    task.remove()