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

def update(id, json):
    with Session() as session:
        person = session.get(tables.Person, id)
        if person == None:
            return None
        else:
            person.update(json)
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

def list():
    with Session() as session:
        statement = select(tables.Person) \
                    .order_by(tables.Person.created) \
                    .limit(10)
        
        results = session.scalars(statement).all()

        output = []
        for result in results:
            output.append(result.to_dict())

        return output