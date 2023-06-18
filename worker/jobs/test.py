import logging


def dispatch(task):
    task.handler = "test"
    logging.info("dispatching: %s", task)

    if task.name == "error":
        raise Exception("this test exception is expected")

    task.remove()