import logging
from operator import itemgetter
from db import tables
from .helpers import define_crud


add, get, update, remove, query = itemgetter(
    "add", "get", "update", "remove", "query"
)(define_crud(tables.Registration))