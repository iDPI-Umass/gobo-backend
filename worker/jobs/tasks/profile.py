import logging
import models
from .stale import handle_stale
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


@handle_stale
def get_profile(task):
    client = h.enforce("client", task)
    profile = client.get_profile()
    return {"profile": profile}

def map_profile(task):
    client = h.enforce("client", task)
    identity = h.enforce("identity", task)
    profile = h.enforce("profile", task)
    identity = client.map_profile({
        "profile": profile, 
        "identity": identity
    })
    return {"identity": identity}


def upsert_profile(task):
    identity = h.enforce("identity", task)
    models.identity.upsert(identity)