import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

GoboKey = tables.GoboKey


add, get, update, remove, query, find, pluck = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pluck"
)(define_crud(GoboKey))