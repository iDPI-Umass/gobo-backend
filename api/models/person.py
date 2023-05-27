import logging
from db import tables
from db.base import Session

def add(data):
    with Session() as session:
        person = tables.Person(**data)
        session.add(person)
        session.commit()