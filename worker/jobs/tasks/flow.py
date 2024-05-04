import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


# no-op to create stable start point for all flows.
# TODO: Maybe add some instrumentation here later.
def start_flow(task):
    pass


def flow_update_identity(task):
    identity = h.enforce("identity", task)
    platform = h.get_platform(identity)
    is_onboarding = task.details.get("is_onboarding", False)

    queues.default.put_flow(task.priority, [
        {
            "queue": platform, 
            "name": "get client",
            "details": {
                "identity": identity,
                "is_shallow": is_onboarding
            }
        },
        {
            "queue": platform, 
            "name": "get profile",
        },
        {
            "queue": "default", 
            "name": "map profile",
        },
        {
            "queue": "default", 
            "name": "upsert profile"
        },
        {
            "queue": "default",
            "name": "filter publish only"  
        },
        {
            "queue": platform, 
            "name": "pull sources",
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
            "name": "flow - pull notifications"
        },
        {
            "queue": "default",
            "name": "flow - update identity feed"
        }
    ])

def flow_update_identity_feed(task):
    identity = h.enforce("identity", task)
    platform = h.get_platform(identity)
    client = h.enforce("client", task)
    sources = h.enforce("sources", task)
    is_shallow = task.details.get("is_shallow", False)

    for source in sources:
        queues.default.put_flow(
            priority = task.priority,
            failure = h.rollback_cursor,
            flow = [
            {
                "queue": "default",
                "name": "check source lockout",
                "details": {
                    "platform": platform,
                    "identity": identity,
                    "client": client,
                    "source": source
                }
            },
            {
                "queue": "default",
                "name": "get source cursor"
            },
            {
                "queue": platform, 
                "name": "pull posts",
                "details": {"is_shallow": is_shallow}
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
                "name": "upsert posts"
            },
        ])


def flow_onboard_identity(task):
    identity = h.enforce("identity", task)

    queues.default.put_details("flow - update identity", {
        "priority": task.priority,
        "identity": identity,
        "is_onboarding": True
    })



def flow_pull_sources(task):
    identity = h.enforce("identity", task)
    platform = h.get_platform(identity)
    client = task.details.get("client")

    queues.default.put_flow(task.priority, [
        {
            "queue": platform, 
            "name": "get client",
            "details": {
                "identity": identity,
                "client": client
            }
        },
        {
            "queue": platform, 
            "name": "pull sources",
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
    client = task.details.get("client")

    queues.default.put_flow(
        priority = task.priority,
        failure = h.rollback_cursor,
        flow = [
        {
            "queue": platform, 
            "name": "get client",
            "details": {
                "identity": identity,
                "client": client
            }
        },
        {
            "queue": "default",
            "name": "check source lockout",
            "details": {"source": source}
        },
        {
            "queue": "default",
            "name": "get source cursor"
        },
        {
            "queue": platform, 
            "name": "pull posts",
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
            "name": "upsert posts"
        },
    ])



def flow_pull_notifications(task):
    identity = h.enforce("identity", task)
    platform = h.get_platform(identity)
    client = task.details.get("client")

    queues.default.put_flow(
        priority = task.priority,
        failure = h.rollback_cursor,
        flow = [
        {
          "queue": platform,
          "name": "get client",
          "details": {
              "identity": identity,
              "client": client
          }
        },
        {
            "queue": "default",
            "name": "get notification cursor"
        },
        {
            "queue": platform, 
            "name": "pull notifications"
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
            "name": "map posts"
        },
        {
            "queue": "default", 
            "name": "upsert posts"
        },
        {
            "queue": "default", 
            "name": "map notifications"
        },
        {
            "queue": "default", 
            "name": "upsert notifications"
        }
    ])


def flow_dismiss_notification(task):
    identity = h.enforce("identity", task)
    platform = h.get_platform(identity)
    notification_id = h.enforce("notification_id", task)

    queues.default.put_flow(
        priority = task.priority,
        flow = [
        {
          "queue": platform,
          "name": "get client",
          "details": {
              "identity": identity,
              "notification_id": notification_id
          }
        },
        {
            "queue": platform, 
            "name": "dismiss notification"
        }
    ])


# TODO: Does this belong in its own module category?
def filter_publish_only(task):
    identity = h.enforce("identity", task)
    
    excluded = ["linkedin"]
    if h.has_platform(excluded, identity):
        task.halt()
        return