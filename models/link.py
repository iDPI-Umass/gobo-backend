import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Link = tables.Link

add, get, update, remove, query, find, pull, random = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull", "random"
)(define_crud(Link))


def upsert(data):
    with Session() as session:
        if data.get("origin_type") is None:
            raise Exception("upsert requires link have origin_type")
        if data.get("origin_id") is None:
            raise Exception("upsert requires link have origin_id")
        if data.get("target_type") is None:
            raise Exception("upsert requires link have target_type")
        if data.get("target_id") is None:
            raise Exception("upsert requires link have target_id")
        if data.get("name") is None:
            raise Exception("upsert requires link have name")


        statement = select(Link) \
            .where(Link.origin_type == data["origin_type"]) \
            .where(Link.origin_id == data["origin_id"]) \
            .where(Link.target_type == data["target_type"]) \
            .where(Link.target_id == data["target_id"]) \
            .where(Link.name == data["name"])

        # Secondary is not mandatory, but we need to avoid creating another
        # link if there is a match on this dimension.
        if data.get("secondary") is not None:
            statement = statement.where(Link.secondary == data["secondary"])
          
        statement = statement.limit(1)
       
        row = session.scalars(statement).first()

        if row == None:
            row = Link.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()
  

def find_and_remove(data):
    with Session() as session:
        statement = select(Link)
        for key, value in data.items():
            statement = statement.where(getattr(Link, key) == value)
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            return None
        else:
            session.delete(row)
            session.commit()
            return row.to_dict()