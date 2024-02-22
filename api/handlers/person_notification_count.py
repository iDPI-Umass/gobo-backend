import logging
from flask import request
import http_errors
import models
from .helpers import get_viewer

class NotificationCounter():
    def __init__(self, person_id):
        self.person_id = person_id
        self.loop = None
        self.counter = models.counter.LoopCounter(
            "person",
            person_id, 
            "person-notification-count"
        )

    def to_resource(self):
        resource = self.counter.to_resource(self.loop)
        resource["person_id"] = self.person_id
        return resource
    
    def set(self, value):
        self.loop = self.counter.set(value)
        


def get_counter(person_id):
    return 

def person_notification_count_get(person_id):
    get_viewer(person_id)
    
    counter = NotificationCounter(person_id)
    return counter.to_resource()


def person_notification_count_put(person_id):
    get_viewer(person_id)
    _person = request.json.get("person_id")
    if person_id != _person:
        raise http_errors.unprocessable_content(
            f"person_id {person_id} does not match resource in body, rejecting"
        )
    
    counter = NotificationCounter(person_id)
    counter.set(request.json.get("count"))
    return counter.to_resource()