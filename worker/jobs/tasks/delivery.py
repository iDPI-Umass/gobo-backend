import logging
from functools import wraps
import models
from . import helpers as h


def handle_delivery(f):
    @wraps(f)
    def decorated(task):
        target = h.enforce("target", task)
        stash = target.get("stash")
        if stash is None:
            stash = {}
         
        try:
            result = f(task)
            stash["references"] = result
            target["state"] = "delivered"
            target["stash"] = stash
            models.delivery_target.update(target["id"], target)
            return result
        except Exception as e:
            # TODO: Make this more sophisticated an provide some sort of 
            #   relevant feedback to people. Especially for 503 class errors.
            target["state"] = "error"
            models.delivery_target.update(target["id"], target)
            raise e
        
    return decorated


def handle_unpublish(f):
    @wraps(f)
    def decorated(task):
        target = h.enforce("target", task)
         
        try:
            result = f(task)
            target["state"] = "unpublished"
            models.delivery_target.update(target["id"], target)
            return result
        except Exception as e:
            # TODO: Make this more sophisticated an provide some sort of 
            #   relevant feedback to people. Especially for 503 class errors.
            target["state"] = "delivered"
            models.delivery_target.update(target["id"], target)
            raise e
        
    return decorated