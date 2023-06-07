import logging
from operator import itemgetter
from db import tables
from .helpers import define_crud


add, get, update, remove, query, find, conditional_remove = itemgetter(
    "add", "get", "update", "remove", "query", "find", "conditional_remove"
)(define_crud(tables.Identity))