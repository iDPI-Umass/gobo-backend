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