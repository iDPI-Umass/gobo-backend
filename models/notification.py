import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Notification = tables.Notification
Link = tables.Link

add, get, update, remove, query, find, pluck, pull, scan = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pluck", "pull", "scan"
)(define_crud(Notification))


def upsert(data):
    with Session() as session:
        if data.get("base_url") is None or data.get("platform_id") is None:
            raise Exception("upsert requires notification to have base_url and platform_id")

        statement = select(Notification) \
            .where(Notification.base_url == data["base_url"]) \
            .where(Notification.platform_id == data["platform_id"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Notification.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()

def get_cursor(identity_id):
    with Session() as session:
        statement = select(Link) \
            .where(Link.origin_type == "identity") \
            .where(Link.origin_id == identity_id) \
            .where(Link.target_type == "identity") \
            .where(Link.target_id == identity_id) \
            .where(Link.name == "read-cursor-notification") \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Link.write({
                "origin_type": "identity",
                "origin_id": identity_id,
                "target_type": "identity",
                "target_id": identity_id,
                "name": "read-cursor-notification"
            })
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()