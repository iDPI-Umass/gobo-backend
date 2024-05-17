import logging
from functools import wraps
import models
from . import helpers as h


def handle_delivery(f):
    @wraps(f)
    def decorated(task):
        delivery_target = h.enforce("delivery_target", task)
         
        try:
            result = f(task)
            delivery_target["state"] = "delivered"
            models.delivery_target.upsert(delivery_target)
            return result
        except Exception as e:
            # TODO: Make this more sophisticated an provide some sort of 
            #   relevant feedback to people. Especially for 503 class errors.
            delivery_target["state"] = "error"
            models.delivery_target.upsert(delivery_target)
            raise e
        
    return decorated