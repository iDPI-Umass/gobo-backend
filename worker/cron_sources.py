import logging
import schedule
import joy
import queues
from .task import Task


def start_sources():
    schedule.every(12).hours.do(
        queues.twitter.put, Task("start pull posts")
    )