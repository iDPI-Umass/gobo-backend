import logging
import json
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Delivery = tables.Delivery
Link = tables.Link

add, get, remove, query, find,  = itemgetter(
    "add", "get", "remove", "query", "find"
)(define_crud(Delivery))


def fetch(id):
    delivery = get(id)
    if delivery is None:
        return None

    targets = []
    with Session() as session:
        statement = select(Link) \
            .where(Link.origin_type == "delivery") \
            .where(Link.origin_id == id) \
            .where(Link.target_type == "identity") \
            .where(Link.name == "delivers")

        rows = session.scalars(statement).all()
        for row in rows:
            stash = json.loads(row.secondary)
            stash["identity"] = row.target_id
            targets.append(stash)

    delivery["targets"] = targets
    logging.info(delivery)
    return delivery
    

def update(id, identity_id, data):
    link = {
        "origin_type": "delivery",
        "origin_id": id,
        "target_type": "identity",
        "target_id": identity_id,
        "name": "delivers",
        "secondary": json.dumps(data)
    }


    with Session() as session:
        statement = select(Link) \
            .where(Link.origin_type == "delivery") \
            .where(Link.origin_id == id) \
            .where(Link.target_type == "identity") \
            .where(Link.target_id == identity_id) \
            .where(Link.name == "delivers") \
            .limit(1)

        row = session.scalars(statement).first()
        if row == None:
            row = Link.write(link)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(link)
            session.commit()
            return row.to_dict()
    