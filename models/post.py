import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Post = tables.Post
Link = tables.Link


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


def view_feed(data):
    with Session() as session:
        if data["direction"] == "descending":
            attribute = Link.secondary.desc()
        else:
            attribute = Link.secondary

        if data["page"] == 1:
            offset = None
        else:
            offset = (data["page"] - 1) * data["per_page"]

        statement = select(Link) \
                    .where(Link.origin_type == "person") \
                    .where(Link.origin_id == data["person_id"]) \
                    .where(Link.target_type == "post") \
                    .where(Link.name == f"{data['view']}-feed") \
                    .order_by(attribute) \
                    .offset(offset) \
                    .limit(data["per_page"])

        rows = session.scalars(statement).all()

        if len(rows) == 0:
            return []
        else:
            ids = []
            id_index = {}
            i = 0
            for row in rows:
                ids.append(row.target_id)
                id_index[row.target_id] = i
                i += 1


            statement = select(Post).where(Post.id.in_(ids))
            rows = session.scalars(statement).all()
            results = [None] * len(rows)
            for row in rows:
                i = id_index[row.id]
                results[i] = row.to_dict()
            
            return results