import logging
import time
from operator import itemgetter
from sqlalchemy import select
from db import tables
from db.base import Session
from .helpers import define_crud

LinkedinSession = tables.LinkedinSession

add, get, update, remove, query, find = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(LinkedinSession))


def upsert(data):
    with Session() as session:
        if data.get("identity_id") is None:
            raise Exception("upsert requires LinkedinSession have identity_id")

        statement = select(LinkedinSession) \
            .where(LinkedinSession.identity_id == data["identity_id"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row is None:
            row = LinkedinSession.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()