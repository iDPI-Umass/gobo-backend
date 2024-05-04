import logging
import models
from . import helpers as h
from .stale import handle_stale

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator




@handle_stale
def get_client(task):
    if task.details.get("client") is not None:
        return
    else:
      identity = h.enforce("identity", task)
      client = h.get_client(identity)
      client.login()
      return {"client": client}