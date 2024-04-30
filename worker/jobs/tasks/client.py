import logging
import models
import joy
import queues
from . import helpers as h
from clients import HTTPError

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


# See get_profile for more on the identity error handling. We need to do it
# here for Bluesky because we sometimes need to make an HTTP request when
# instantiating the client to refresh a stale session.


def get_client(task):
    if task.details.get("client") is not None:
        return
    else:
      identity = h.enforce("identity", task)
      client = h.get_client(identity)
      
      
      try:
          client.login()
      
      except HTTPError as e:
        if identity["platform"] == "bluesky" and e.status == 400:
            error = e.body.get("error")
            if error == "ExpiredToken":
                logging.warning("detected revoked Bluesky token, removing session and identity")
                queues.default.put_details(
                    priority = 1,
                    name = "stale identity",
                    details = {"identity": identity}
                )
                return
            raise e
      
      
      
      return {"client": client}