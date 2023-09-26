import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


# Noop to create stable start point for all flows. Maybe add some instrumentation here later.
def start_flow(task):
    pass


def flow_pull_sources(task):
    identity = h.enforce("identity", task)
    platform = h.get_platform(identity)

    queues.default.put_flow([
        {
            "queue": platform, 
            "name": "pull sources",
            "details": {"identity": identity}
        },
        {
            "queue": "default", 
            "name": "map sources"
        },
        {
            "queue": "default", 
            "name": "upsert sources"
        },
        {
            "queue": "default", 
            "name": "reconcile sources",
        }
    ])



def flow_pull_posts(task):
    identity = h.enforce("identity", task)
    source = h.enforce("source", task)
    platform = h.get_platform(source)

    queues.default.put_flow([
        {
            "queue": "default",
            "name": "get last retrieved",
            "details": {"source": source}
        },
        {
            "queue": platform, 
            "name": "pull posts",
            "details": {"identity": identity}
        },
        {
            "queue": "default", 
            "name": "map sources"
        },
        {
            "queue": "default", 
            "name": "upsert sources",
        },
        {
            "queue": "default", 
            "name": "map posts"
        },
        {
            "queue": "default", 
            "name": "set last retrieved"
        },
        {
            "queue": "default", 
            "name": "upsert posts"
        },
    ])


def flow_onboard_sources(task):
    identity = h.enforce("identity", task)
    platform = h.get_platform(identity)

    queues.default.put_flow([
        {
            "queue": platform, 
            "name": "pull sources",
            "details": {"identity": identity}
        },
        {
            "queue": "default", 
            "name": "map sources"
        },
        {
            "queue": "default", 
            "name": "upsert sources"
        },
        {
            "queue": "default", 
            "name": "reconcile sources",
        },
        {
            "queue": "default",
            "name": "flow - onboard source posts"
        }
    ])


def flow_onboard_source_posts(task):
    identity = h.enforce("identity", task)
    sources = h.enforce("sources", task)
    platform = h.get_platform(identity)

    for source in sources:
        queues.default.put_flow([
            {
                "queue": "default",
                "name": "get last retrieved",
                "details": {"source": source}
            },
            {
                "queue": platform, 
                "name": "pull posts",
                "details": {
                    "identity": identity,
                    "is_shallow": True
                }
            },
            {
                "queue": "default", 
                "name": "map sources"
            },
            {
                "queue": "default", 
                "name": "upsert sources",
            },
            {
                "queue": "default", 
                "name": "map posts"
            },
            {
                "queue": "default", 
                "name": "set last retrieved"
            },
            {
                "queue": "default", 
                "name": "upsert posts"
            },
        ])




