import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud


add, get, update, remove, query, find, conditional_remove = itemgetter(
    "add", "get", "update", "remove", "query", "find", "conditional_remove"
)(define_crud(tables.Link))


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