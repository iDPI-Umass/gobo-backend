import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
import joy
from .helpers import define_crud

Link = tables.Link

add, get, update, remove, query, find, pull, random = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull", "random"
)(define_crud(Link))


def upsert(data):
    with Session() as session:
        if data.get("origin_type") is None:
            raise Exception("upsert requires link have origin_type")
        if data.get("origin_id") is None:
            raise Exception("upsert requires link have origin_id")
        if data.get("target_type") is None:
            raise Exception("upsert requires link have target_type")
        if data.get("target_id") is None:
            raise Exception("upsert requires link have target_id")
        if data.get("name") is None:
            raise Exception("upsert requires link have name")


        statement = select(Link) \
            .where(Link.origin_type == data["origin_type"]) \
            .where(Link.origin_id == data["origin_id"]) \
            .where(Link.target_type == data["target_type"]) \
            .where(Link.target_id == data["target_id"]) \
            .where(Link.name == data["name"])

        # Secondary is not mandatory, but we need to avoid creating another
        # link if there is a match on this dimension.
        if data.get("secondary") is not None:
            statement = statement.where(Link.secondary == data["secondary"])
          
        statement = statement.limit(1)
       
        row = session.scalars(statement).first()

        if row == None:
            row = Link.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()
  

def find_and_remove(data):
    with Session() as session:
        statement = select(Link)
        for key, value in data.items():
            statement = statement.where(getattr(Link, key) == value)
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            return None
        else:
            session.delete(row)
            session.commit()
            return row.to_dict()




# We use the database as a blackboard to store read cursors for readers
# that are possibly spread across processes.
def get_cursor(origin_type, origin_id, name, target_type = None, target_id = None):
    if target_type is None:
        target_type = origin_type
    if target_id is None:
        target_id = origin_id

    with Session() as session:
        statement = select(Link) \
            .where(Link.origin_type == origin_type) \
            .where(Link.origin_id == origin_id) \
            .where(Link.target_type == target_type) \
            .where(Link.target_id == target_id) \
            .where(Link.name == name) \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Link.write({
                "origin_type": origin_type,
                "origin_id": origin_id,
                "target_type": target_type,
                "target_id": target_id,
                "name": name
            })
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()
        

# We also need to add a mutex affordance so that we can make sure only one read
# is authoritative. This prevents a "stampede" of requests to provider
# platforms if that's they depend on this coordination.
# In the comments below, "source" refers to any external resource we need to fetch.
def stamp_cursor(id, timeout):
    now = joy.time.now()
    nowdate = joy.time.convert("iso", "date", now)
    with Session() as session:
        statement = select(Link) \
           .where(Link.id == id) \
           .with_for_update(nowait = False)
           
        row = session.scalars(statement).first() 
        edge = row.to_dict()
        stored_time = edge.get("secondary")
        
        # Unread source. Stamp it with now and signal a fresh read.
        if stored_time is None:
            edge["secondary"] = now
            row.update(edge)
            session.commit()
            return None
        
        stored_date = joy.time.convert("iso", "date", stored_time)
        delta = nowdate - stored_date

        # Source is available for read. Stamp it with now and signal scoped read.
        if delta.total_seconds() > timeout:
            edge["secondary"] = now
            row.update(edge)
            session.commit()
            return stored_time
        
        # Source is not available for read. Signal to bail.
        else:
            return False



class LoopCursor():
    def __init__(self, origin_type, origin_id, name):
        self.loop = get_cursor(origin_type, origin_id, name)
        self.last_retrieved = None
    
    # This carefully fetches the cursor's value from a transactional read.
    # The function is called "stamp" because we're also providing an optimistic
    # provisional update to the cursor. We save it in the class's scope
    # so that we can rollback the cursor if we detect failure.
    def stamp(self, timeout):
        cursor = stamp_cursor(self.loop["id"], timeout)
        if isinstance(cursor, str):
            self.last_retrieved = cursor
        return cursor

    def update(self, time):
        self.loop["secondary"] = time
        update(self.loop["id"], self.loop)
  
    # We detected a failure and need to roll back the timestamp
    def rollback(self):
        if self.last_retrieved is not None:
            self.update(self.last_retrieved)