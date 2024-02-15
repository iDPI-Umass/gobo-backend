import logging
import models
import joy
import queues
from . import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def fanout_update_identity(task):
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
        queues.default.put_details(
            name = "flow - update identity",
            priority = task.priority,
            details = {"identity": identity}
        )



def fanout_update_notifications(task):
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
        queues.default.put_details(
            name = "flow - update notifications",
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