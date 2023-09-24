import logging
import models
import joy
import queues
import helpers as h

where = models.helpers.where
build_query = models.helpers.build_query
QueryIterator = models.helpers.QueryIterator


def find_identity(session):
    return models.identity.find({
        "person_id": session["person_id"],
        "base_url": session["base_url"],
        "platform_id": session["did"]
    })


def cycle_blusky_sessions(task):
    access_limit = joy.time.to_iso_string(joy.time.hours_from_now(0.5))
    refresh_limit = joy.time.to_iso_string(joy.time.hours_from_now(24))

    sessions = QueryIterator(
        model = models.bluesky_session,
        wheres = [
            where("refresh_expires", refresh_limit, "lt")
        ]
    )

    for session in sessions:
        identity = find_identity(session)
        queues.bluesky.put_details("cycle refresh token", {
            "identity": identity,
            "session": session
        })
      

    sessions = QueryIterator(
        model = models.bluesky_session,
        wheres = [
            where("access_expires", access_limit, "lt")
        ]
    )

    for session in sessions:
        identity = find_identity(session)
        queues.bluesky.put_details("cycle access token", {
            "identity": identity,
            "session": session
        })