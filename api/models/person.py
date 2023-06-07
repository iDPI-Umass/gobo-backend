import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Person = tables.Person
Link = tables.Link
tableDict = {
  
}


add, get, update, remove, query, find, conditional_remove = itemgetter(
    "add", "get", "update", "remove", "query", "find", "conditional_remove"
)(define_crud(Person))

# Because the person table records are implicity created via our connection
# to Auth0, this special method will lookup a person based on that authority_id,
# or create a new person on the spot.
def lookup(authority_id):
    with Session() as session:
        statement = select(Person)
        statement = statement.where(getattr(Person, "authority_id") == authority_id)
        statement = statement.limit(1)
        row = session.scalars(statement).first()

        if row == None:
            row = Person(authority_id=authority_id)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()

def get_links(Table, data):
    with Session() as session:
        if data["page"] == 1:
            offset = None
        else:
            offset = (data["page"] - 1) * data["per_page"]

        statement = select(Link) \
                    .where(Link.origin_type == "person") \
                    .where(Link.origin_id == data["id"]) \
                    .where(Link.target_type == data["resource"]) \
                    .order_by(Link.created.desc()) \
                    .offset(offset) \
                    .limit(data["per_page"])

        rows = session.scalars(statement).all()

        if len(rows) == 0:
            return []
        else:
            ids = []
            for row in rows:
                ids.append(row.id)

            statement = select(Table).where(Table.id in ids)
            rows = session.scalars(statement).all()
            results = []
            for row in rows:
                results.append(row.to_dict())
            return results