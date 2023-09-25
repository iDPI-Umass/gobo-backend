import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def follow_fanout(task):
    platform = h.get_platform(task.details)
  
    if platform == "all":
        wheres = []
    else:
        wheres = [where("platform", platform)]

    identities = QueryIterator(
        model = models.identity,
        wheres = wheres
    )
    for identity in identities:
        queues.default.put_details("flow pull sources", {"identity": identity})


def pull_sources(task):
    identity = h.enforce("identity", task)
    client = h.get_client(identity)
    client.login()
    return {"graph": client.list_sources()}


def map_sources(task):
    identity = h.enforce("identity", task)
    graph = h.enforce("graph", task)
    client = h.get_client(identity)
    sources = client.map_sources(graph)
    return {"sources": sources}


def upsert_sources(task):
    _sources = h.enforce("sources", task)
    sources = []
    for _source in _sources:
        source = models.source.upsert(_source)
        sources.append(source)
    return {"sources": sources}


def reconcile_sources(task):
    identity = h.enforce("identity", task)
    sources = h.enforce("sources", task)
    h.reconcile_sources(identity, sources)
    return {"sources": sources}