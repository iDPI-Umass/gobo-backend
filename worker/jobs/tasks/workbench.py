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
    identity = models.identity.get(454)
    queues.default.put_details("flow - pull notifications", {
        "identity": identity
    })

# def workbench(task):
#     identity = models.identity.get(500)
#     notification_id = 27
#     queues.default.put_details("flow - dismiss notification", {
#         "identity": identity,
#         "notification_id": notification_id
#     })