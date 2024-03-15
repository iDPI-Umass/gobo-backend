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
    identity = models.identity.get(501)
    queues.default.put_details("flow - update identity", {
        "identity": identity
    })