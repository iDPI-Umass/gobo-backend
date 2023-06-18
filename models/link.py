import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud


add, get, update, remove, query, find, pull = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull"
)(define_crud(tables.Link))


def safe_add(data):
    with Session() as session:
        statement = select(tables.Link)
        for key, value in data.items():
            statement = statement.where(getattr(tables.Link, key) == value)
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = tables.Link.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()

def upsert(data):
    with Session() as session:
        statement = select(tables.Link)
        for key, value in data.items():
            statement = statement.where(getattr(tables.Link, key) == value)
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = tables.Link.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()
  

def find_and_remove(data):
    with Session() as session:
        statement = select(tables.Link)
        for key, value in data.items():
            statement = statement.where(getattr(tables.Link, key) == value)
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            return
        else:
            session.delete(row)
            session.commit()
            return