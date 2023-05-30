import logging
from sqlalchemy import select
from db import tables
from db.base import Session


def add(data):
    with Session() as session:
        person = tables.Person(**data)
        session.add(person)
        session.commit()
        return person.to_dict()

def get(id):
    with Session() as session:
        person = session.get(tables.Person, id)
        if person == None:
            return None
        else:
            return person.to_dict()

def update(id, data):
    with Session() as session:
        person = session.get(tables.Person, id)
        if person == None:
            return None
        else:
            person.update(data)
            session.commit()
            return person.to_dict()

def delete(id):
    with Session() as session:
        person = session.get(tables.Person, id)
        if person == None:
            return None
        else:
            session.delete(person)
            session.commit()
            return person.to_dict()

def list(data):
    with Session() as session:  
        if data["direction"] == "descending":
            attribute = getattr(tables.Person, data["view"]).desc()
        else:
            attribute = getattr(tables.Person, data["view"])

        if data["page"] == 1:
            offset = None
        else:
            offset = (data["page"] - 1) * data["per_page"]

        statement = select(tables.Person) \
                    .order_by(attribute) \
                    .offset(offset) \
                    .limit(data["per_page"])

        rows = session.scalars(statement).all()

        results = []
        for row in rows:
            results.append(row.to_dict())
        return results