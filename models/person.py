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


add, get, update, remove, query, find = itemgetter(
    "add", "get", "update", "remove", "query", "find"
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
        person_id = data["person_id"]
        resource = data["resource"]

        if data["page"] == 1:
            offset = None
        else:
            offset = (data["page"] - 1) * data["per_page"]

        statement = select(Link) \
                    .where(Link.origin_type == "person") \
                    .where(Link.origin_id == person_id) \
                    .where(Link.target_type == resource) \
                    .where(Link.name == f"has-{resource}") \
                    .order_by(Link.created.desc()) \
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


            statement = select(Table).where(Table.id.in_(ids))
            rows = session.scalars(statement).all()
            results = [None] * len(rows)
            for row in rows:
                i = id_index[row.id]
                results[i] = row.to_dict()
            
            return results

def pull_links(Table, data):
    page = data.get("page") or 1
    data["page"] = page
    per_page = data.get("per_page") or 500
    data["per_page"] = per_page

    results = []
    while True:
        _results = get_links(Table, data)
        results.extend(_results)
        if len(_results) != per_page:
            break
        else:
            page = page + 1
            data["page"] = page

    return results

def pull_sources(id):
    data = {
      resource: "sources"
    }