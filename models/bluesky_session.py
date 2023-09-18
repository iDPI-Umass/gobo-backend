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
        if data.get("person_id") is None:
            raise Exception("upsert requires BlueskySession have person_id")
        if data.get("base_url") is None:
            raise Exception("upsert requires BlueskySession have base_url")
        if data.get("did") is None:
            raise Exception("upsert requires BlueskySession have did")

        statement = select(BlueskySession) \
            .where(BlueskySession.person_id == data["person_id"]) \
            .where(BlueskySession.base_url == data["base_url"]) \
            .where(BlueskySession.did == data["did"]) \
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