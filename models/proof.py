import logging
import os
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Proof = tables.Proof

add, get, update, remove, query, find, pull, random = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull", "random"
)(define_crud(Proof))
