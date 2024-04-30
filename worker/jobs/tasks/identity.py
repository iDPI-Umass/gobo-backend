import logging
import joy
import models
import queues
from . import helpers as h

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def remove_identity(task):
    identity = h.enforce("identity", task)
    h.remove_identity(identity)

def stale_identity(task):
    identity = h.enforce("identity", task)
    h.stale_identity(identity)