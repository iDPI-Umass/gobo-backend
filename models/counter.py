import logging
from operator import itemgetter
from sqlalchemy import select
from sqlalchemy import update as Update
from db.base import Session
from db import tables
import joy
from .helpers import define_crud

Counter = tables.Counter

add, get, update, remove, query, find, pull, random = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull", "random"
)(define_crud(Counter))


# Similar to cursors, counters are useful for orchestration. However, they
# use integer type secondary column to support atomic updates.



def upsert(data):
    with Session() as session:
        if data.get("origin_type") is None:
            raise Exception("upsert requires counter have origin_type")
        if data.get("origin_id") is None:
            raise Exception("upsert requires counter have origin_id")
        if data.get("target_type") is None:
            raise Exception("upsert requires counter have target_type")
        if data.get("target_id") is None:
            raise Exception("upsert requires counter have target_id")
        if data.get("name") is None:
            raise Exception("upsert requires counter have name")


        statement = select(Counter) \
            .where(Counter.origin_type == data["origin_type"]) \
            .where(Counter.origin_id == data["origin_id"]) \
            .where(Counter.target_type == data["target_type"]) \
            .where(Counter.target_id == data["target_id"]) \
            .where(Counter.name == data["name"]) \
            .limit(1)
       
        row = session.scalars(statement).first()

        if row == None:
            row = Counter.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()
  

def touch(origin_type, origin_id, name, target_type = None, target_id = None):
    if target_type is None:
        target_type = origin_type
    if target_id is None:
        target_id = origin_id

    with Session() as session:
        statement = select(Counter) \
            .where(Counter.origin_type == origin_type) \
            .where(Counter.origin_id == origin_id) \
            .where(Counter.target_type == target_type) \
            .where(Counter.target_id == target_id) \
            .where(Counter.name == name) \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Counter.write({
                "origin_type": origin_type,
                "origin_id": origin_id,
                "target_type": target_type,
                "target_id": target_id,
                "name": name,
                "secondary": 0
            })
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()


def update_counter(id, amount): 
    with Session() as session:
        statement = Update(Counter) \
            .where(Counter.id == id) \
            .values(secondary = Counter.secondary + amount)

        session.execute(statement)
        session.commit()
        return


class LoopCounter():
    def __init__(self, type, id, name):
        self.type = type
        self.id = id
        self.name = name
        self.count = 0
    
    def increment(self, value = 1):
        self.count += value

    def decrement(self, value = 1):
        self.count -= value

    def save(self):
        if self.count > 0:
            loop = touch(self.type, self.id, self.name)
            update_counter(loop["id"], self.count)
            self.count = 0

    def set(self, value):
        return upsert({
            "origin_type": self.type,
            "origin_id": self.id,
            "name": self.name,
            "target_type": self.type,
            "target_id": self.id,
            "secondary": value
        })
    
    def get(self):
        loop = touch(self.type, self.id, self.name)
        return loop.get("secondary", 0)

    def to_resource(self, loop = None):
        if loop is None:
            loop = touch(self.type, self.id, self.name)
        return {
          "count": loop.get("secondary", 0),
          "created": loop["created"],
          "updated": loop["updated"]  
        }