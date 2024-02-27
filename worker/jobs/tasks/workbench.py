import logging
import joy
import models
import queues
from . import helpers as h
from . import notification as Notification

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def test(task):
    logging.info(task.details)

def workbench(task):
    # person_id = 500
    # counter = models.counter.LoopCounter(
    #     "person",
    #     person_id, 
    #     "person-notification-count"
    # )

    # # counter.increment()
    # # counter.increment()
    # # counter.save()
    # counter.set(0)
    # logging.info(counter.to_resource())

    # return

    identity = models.identity.get(501)
    queues.default.put_details("flow - pull notifications", {
        "identity": identity
    })