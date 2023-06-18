import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Post = tables.Post


add, get, update, remove, query, find = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(Post))


def safe_add(data):
    with Session() as session:
        statement = select(Post)
        statement = statement.where(Post.url == data["url"])
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Post.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()

def upsert(data):
    with Session() as session:
        statement = select(Post)
        statement = statement.where(Post.url == data["url"])
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Post.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()