import logging
import models
import joy
import queues
from . import helpers as h
from .stale import handle_stale

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def check_source_lockout(task):
    source = h.enforce("source", task)
    handle = models.link.Lockout("source", source["id"], "source-lockout")
    if handle.read() is not None:
        task.halt()


def get_source_cursor(task):
    source = h.enforce("source", task)    
    platform = h.enforce("platform", task)
    name = "read-cursor-source"

    if platform == "reddit":
        timeout = 1200
    else:
        timeout = 120

    cursor = models.cursor.LoopCursor("source", source["id"], name)
    last_retrieved = cursor.stamp(timeout)

    # If this isn't a viable read, we need to bail.
    if last_retrieved == False:
        task.halt()
        return
    else:
      return {
        "cursor": cursor,
        "last_retrieved": last_retrieved
      }


@handle_stale
def pull_sources(task):
    client = h.enforce("client", task)
    graph = client.list_sources()
    return {"graph": graph}


def map_sources(task):
    client = h.enforce("client", task)
    graph = h.enforce("graph", task)
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
    h.reconcile_sources(task, identity, sources)
    return {"sources": sources}

def remove_source(task):
    source = h.enforce("source", task)
    h.remove_source(source)