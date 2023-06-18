import logging
from operator import itemgetter
from db import tables
from .helpers import define_crud

Identity = tables.Identity

add, get, update, remove, query, find = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(Identity))

def upsert(data):
    with Session() as session:
        statement = select(Identity)
        statement = statement.where(Identity.profile_url == data["profile_url"])
        statement = statement.limit(1)

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