import logging
import joy
import models
import queues
from . import helpers as h

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def test(task):
    logging.info(task.details)

def workbench(task):
    pass