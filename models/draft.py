import logging
import os
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud
from .link import upsert as upsert_link

Draft = tables.Draft

add, get, update, remove, query, find, pull, random = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull", "random"
)(define_crud(Draft))


def submit(delivery, draft):
    upsert_link({
        "origin_type": "delivery",
        "origin_id": delivery["id"],
        "target_type": "draft",
        "target_id": draft["id"],
        "name": "publishes"        
    })

    draft["state"] = "submitted"
    update(draft["id"], draft)