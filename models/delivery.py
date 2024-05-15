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
    return delivery
    

def update(id, identity_id, data):
    link = {
        "origin_type": "delivery",
        "origin_id": id,
        "target_type": "identity",
        "target_id": identity_id,
        "name": "delivers",
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
            link["secondary"] = json.dumps(data)
            row = Link.write(link)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            secondary = json.loads(row.secondary)
            for key, value in data.items():
                secondary[key] = value
            link["secondary"] = json.dumps(secondary)
            row.update(link)
            session.commit()
            return row.to_dict()


def view_person(query):
    with Session() as session:
        feed = []
        lookup = {}

        statement = select(Delivery) \
            .where(Delivery.person_id == query["person_id"])
        
        if query.get("start") is not None:
            statement = statement \
                .where(Delivery.created < query["start"])
        
        statement = statement \
            .order_by(Delivery.created.desc()) \
            .limit(query["per_page"])

        rows = session.scalars(statement).all()
        
        # The next_token tells the client how to try to get the next page.
        if len(rows) == 0:
            next_token = None
        else:
            next_token = rows[-1].created
        
        for row in rows:
            feed.append(row.id)
            lookup[row.id] = row.to_dict()
            lookup[row.id]["targets"] = []






        statement = select(Link) \
            .where(Link.origin_type == "delivery") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "identity") \
            .where(Link.name == "delivers")

        rows = session.scalars(statement).all()
        for row in rows:
            stash = json.loads(row.secondary)
            stash["identity"] = row.target_id
            lookup[row.origin_id]["targets"].append(stash)
    
    # Package the results
    output = {
        "feed": feed,
        "deliveries": list(lookup.values())
    }

    if next_token is not None:
        output["next"] = next_token

    return output