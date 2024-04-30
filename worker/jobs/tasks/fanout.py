import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator

publish_only = [
    "linkedin"
]

# TODO: For identities from providers where we support publish-only behavior,
#   we don't want the fanout task to pick them up and run them through the
#   common read flows. Will this evolve over time?
def fanout_update_identity(task):
    platform = h.get_platform(task.details)

    wheres = [
        where("stale", False)
    ]
  
    if platform != "all":
        wheres.append(where("platform", platform))

    identities = QueryIterator(
        model = models.identity,
        wheres = wheres
    )
    for identity in identities:
        queues.default.put_details(
            name = "flow - update identity",
            priority = task.priority,
            details = {"identity": identity}
        )



def fanout_pull_notifications(task):
    platform = h.get_platform(task.details)
  
    wheres = [
        where("stale", False),
        where("platform", publish_only, "not in")
    ]
  
    if platform != "all":
        wheres.append(where("platform", platform))

    identities = QueryIterator(
        model = models.identity,
        wheres = wheres
    )
    for identity in identities:
        queues.default.put_details(
            name = "flow - pull notifications",
            priority = task.priority,
            details = {"identity": identity}
        )


# def pull_sources_fanout(task):
#     platform = h.get_platform(task.details)
  
#     if platform == "all":
#         wheres = []
#     else:
#         wheres = [where("platform", platform)]

#     identities = QueryIterator(
#         model = models.identity,
#         wheres = wheres
#     )
#     for identity in identities:
#         queues.default.put_details(
#             name = "flow - pull sources",
#             priority = task.priority,
#             details = {"identity": identity}
#         )

# def pull_posts_fanout(task):
#     platform = h.get_platform(task.details)
#     if platform == "all":
#         wheres = []
#     else:
#         wheres = [where("platform", platform)]

#     sources = QueryIterator(
#         model = models.source,
#         wheres = wheres
#     )
#     for source in sources:
#         queues.default.put_details(
#             name = "pull posts from source",
#             priority = task.priority,
#             details = {"source": source}
#         )