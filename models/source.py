import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Source = tables.Source
Link = tables.Link

add, get, update, remove, query, find, pluck = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pluck"
)(define_crud(Source))

def safe_add(data):
    with Session() as session:
        statement = select(Source)
        statement = statement.where(Source.url == data["url"])
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Source.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()

def upsert(data):
    with Session() as session:
        statement = select(Source)
        statement = statement.where(Source.url == data["url"])
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Source.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()

def get_last_retrieved(source_id):
    with Session() as session:
        statement = select(Link) \
            .where(Link.origin_type == "source") \
            .where(Link.origin_id == source_id) \
            .where(Link.target_type == "source") \
            .where(Link.target_id == source_id) \
            .where(Link.name == "last-retrieved") \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Link.write({
                "origin_type": "source",
                "origin_id": source_id,
                "target_type": "source",
                "target_id": source_id,
                "name": "last-retrieved"
            })
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()