import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud


add, get, update, remove, query, find,  = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(tables.Store))

def upsert(data):
    with Session() as session:
        if data.get("person_id") is None:
            raise Exception("upsert requires store have person_id")
        if data.get("name") is None:
            raise Exception("upsert requires store have name")

        statement = select(tables.Store) \
            .where(tables.Store.person_id == data["person_id"]) \
            .where(tables.Store.name == data["name"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row is None:
            row = tables.Store.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()