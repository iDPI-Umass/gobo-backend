import logging
from functools import wraps
import models
from . import helpers as h


def handle_delivery(f):
    @wraps(f)
    def decorated(task):
        delivery = h.enforce("delivery", task)
        identity = h.enforce("identity", task)
         
        try:
            result = f(task)
            models.delivery.update(delivery["id"], identity["id"], {
                "state": "delivered"
            })
            return result
        except Exception as e:
            # TODO: Make this more sophisticated an provide some sort of 
            #   relevant feedback to people. Especially for 503 class errors.
            models.delivery.update(delivery["id"], identity["id"], {
                "state": "error"
            })
            raise e
        
    return decorated