import logging
import json
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud


DeliveryTarget = tables.DeliveryTarget

add, get, remove, query, find,  = itemgetter(
    "add", "get", "remove", "query", "find"
)(define_crud(DeliveryTarget))


def upsert(data):
    with Session() as session:
        if data.get("person_id") is None:
            raise Exception("upsert requires delivery target have person_id")
        if data.get("delivery_id") is None:
            raise Exception("upsert requires delivery target have delivery_id")
        if data.get("identity_id") is None:
            raise Exception("upsert requires delivery target have identity_id")

        statement = select(DeliveryTarget) \
            .where(DeliveryTarget.person_id == data["person_id"]) \
            .where(DeliveryTarget.delivery_id == data["delivery_id"]) \
            .where(DeliveryTarget.identity_id == data["identity_id"]) \
            .limit(1)
        
        row = session.scalars(statement).first()

        if row is None:
            row = DeliveryTarget.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()