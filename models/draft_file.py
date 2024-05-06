import logging
import os
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

DraftFile = tables.DraftFile

add, get, update, remove, query, find, pull, random = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull", "random"
)(define_crud(DraftFile))


def publish(id):
    directory = os.environ.get("UPLOAD_DIRECTORY")
    filename = os.path.join(directory, id)
    if os.path.exists(filename):
        os.remove(filename)
    return remove(id)