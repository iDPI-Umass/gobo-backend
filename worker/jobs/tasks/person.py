import logging
import joy
import models
import queues
from . import helpers as h

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator


def remove_person(task):
    person_id = h.enforce("person_id", task)
    h.remove_person(person_id)