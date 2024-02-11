import logging
import joy
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Source = tables.Source
Link = tables.Link

add, get, update, remove, query, find, pluck, pull, scan = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pluck", "pull", "scan"
)(define_crud(Source))


def upsert(data):
    with Session() as session:
        if data.get("base_url") is None or data.get("platform_id") is None:
            raise Exception("upsert requires source have base_url and platform_id")

        statement = select(Source) \
            .where(Source.base_url == data["base_url"]) \
            .where(Source.platform_id == data["platform_id"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Source.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()

def get_cursor(source_id):
    with Session() as session:
        statement = select(Link) \
            .where(Link.origin_type == "source") \
            .where(Link.origin_id == source_id) \
            .where(Link.target_type == "source") \
            .where(Link.target_id == source_id) \
            .where(Link.name == "read-cursor-source") \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Link.write({
                "origin_type": "source",
                "origin_id": source_id,
                "target_type": "source",
                "target_id": source_id,
                "name": "read-cursor-source"
            })
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()
        

# More sophisticated than notification cursor. Includes mutex on edge value
# that will prevent stampeed requests to provider platforms.
def stamp_cursor(id, timeout):
    now = joy.time.now()
    nowdate = joy.time.convert("iso", "date", now)
    with Session() as session:
        statement = select(Link) \
            .where(Link.id == id) \
            .with_for_update(nowait = False) \
            .limit(1)

        row = session.scalars(statement).first()
        edge = row.to_dict()
        stored_time = edge.get("secondary")
        
        # Unread source. Stamp it with now and signal a fresh read.
        if stored_time is None:
            edge["secondary"] = now
            update(id, edge)
            session.commit()
            return None
        
        stored_date = joy.time.convert("iso", "date", stored_time)
        delta = stored_date - nowdate

        # Source is available for read. Stamp it with now and signal scoped read.
        if delta.total_seconds() > timeout:
            edge["secondary"] = now
            update(id, edge)
            session.commit()
            return stored_time
        
        # Source is not available for read. Signal to bail.
        else:
            return False