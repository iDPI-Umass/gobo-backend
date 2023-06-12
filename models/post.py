import logging
from operator import itemgetter
from db import tables
from .helpers import define_crud


add, get, update, remove, query, find = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(tables.Post))