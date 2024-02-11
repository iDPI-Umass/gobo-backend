import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def get_client(task):
    if task.details.get("client") is not None:
        return
    else:
      identity = h.enforce("identity", task)
      client = h.get_client(identity)
      client.login()
      return {"client": client}