import logging
from operator import itemgetter
from sqlalchemy import select
from db import tables
from db.base import Session
from .helpers import define_crud

Identity = tables.Identity

add, get, update, remove, query, find = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(Identity))

def upsert(data):
    with Session() as session:
        if data.get("profile_url") is None:
            raise Exception("upsert requires identity have profile_url")
        if data.get("person_id") is None:
            raise Exception("upsert requires identity have person_id")

        statement = select(Identity) \
            .where(Identity.profile_url == data["profile_url"]) \
            .where(Identity.person_id == data["person_id"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Identity.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()