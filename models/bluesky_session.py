import logging
import time
from operator import itemgetter
from sqlalchemy import select
from db import tables
from db.base import Session
from .helpers import define_crud

BlueskySession = tables.BlueskySession

add, get, update, remove, query, find = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(BlueskySession))


def upsert(data):
    with Session() as session:
        if data.get("identity_id") is None:
            raise Exception("upsert requires BlueskySession have identity_id")

        statement = select(BlueskySession) \
            .where(BlueskySession.identity_id == data["identity_id"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row is None:
            row = BlueskySession.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()