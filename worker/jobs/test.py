import logging


def dispatch(task):
    if task.name == "error":
        raise Exception("this test exception is expected")

    task.remove()