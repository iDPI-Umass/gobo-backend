import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


class SourceCursor:
    def __init__(self, id):
        self.link = models.source.get_cursor(id)
        self.last_retrieved = None
    
    # This carefully fetches the last read time from a transactional read.
    def stamp(self, timeout):
        cursor = models.source.stamp_cursor(self.link["id"], timeout)
        # We've also made an optimistic, provisional update. Save this value
        # for later if we need to reverse on failure.
        if isinstance(cursor, str):
            self.last_retrieved = cursor
        return cursor

    def update(self, time):
        link = self.link
        link["secondary"] = time
        models.link.update(link["id"], link)
  
    # We detected a failure and need to roll back the timestamp
    def rollback(self):
        if self.last_retrieved is not None:
            self.update(self.last_retrieved)



def get_source_cursor(task):
    source = h.enforce("source", task)    
    platform = h.enforce("platform", task)

    if platform == "reddit":
        timeout = 12000
    else:
        timeout = 120

    cursor = SourceCursor(source["id"])
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