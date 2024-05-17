import logging
import json
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud


Delivery = tables.Delivery
Draft = tables.Draft
DraftFile = tables.DraftFile
DeliveryTarget = tables.DeliveryTarget


add, get, update, remove, query, find,  = itemgetter(
    "add", "get", "update", "remove", "query", "find"
)(define_crud(Delivery))


# Based on https://stackoverflow.com/a/1015405
def bind(instance, f, as_name=None):
    if as_name is None:
        as_name = f.__name__
    bound_method = f.__get__(instance, instance.__class__)
    setattr(instance, as_name, bound_method)
    return bound_method


class FeedBuilder():
    def __init__(self, session):
        self.session = session

        self.feed = []
        self.next_token = None
        self.deliveries = []
        self.drafts = []
        self.files = []
        self.targets = []

    def pull_deliveries(self):
        statement = select(Delivery) \
            .where(Delivery.id.in_(self.feed))
        
        rows = self.session.scalars(statement).all()
        for row in rows:
            self.deliveries.append(row.to_dict())

    def pull_drafts(self):
        seen_drafts = set()
        for delivery in self.deliveries:
            seen_drafts.add(delivery["draft_id"])
        
        statement = select(Draft) \
            .where(Draft.id.in_(list(seen_drafts)))
        
        rows = self.session.scalars(statement).all()
        for row in rows:
            self.drafts.append(row.to_dict())

    def pull_files(self):
        seen_files = set()
        for draft in self.drafts:
            for id in draft["files"]:
                seen_files.add(id)
        
        statement = select(DraftFile) \
            .where(DraftFile.id.in_(list(seen_files)))
        
        rows = self.session.scalars(statement).all()
        for row in rows:
            self.files.append(row.to_dict())

    def pull_targets(self):
        seen_targets = set()
        for delivery in self.deliveries:
            for id in delivery["targets"]:
                seen_targets.add(id)
        
        statement = select(DeliveryTarget) \
            .where(DeliveryTarget.id.in_(list(seen_targets)))
        
        rows = self.session.scalars(statement).all()
        for row in rows:
            self.targets.append(row.to_dict())



def fetch(id):
    def get_primary(self):
        self.feed.append(id)

    with Session() as session:
        builder = FeedBuilder(session)
        bind(builder, get_primary)

        builder.get_primary()
        builder.pull_deliveries()
        builder.pull_drafts()
        builder.pull_files()
        builder.pull_targets()

        # Collate it all for a response graph.
        return {
            "feed": builder.feed,
            "deliveries": builder.deliveries,
            "drafts": builder.drafts,
            "files": builder.files,
            "targets": builder.targets
        }



def view_person(query):
    def get_primary(self):
        statement = select(Delivery) \
            .where(Delivery.person_id == query["person_id"])
        
        if query.get("start") is not None:
            statement = statement \
                .where(Delivery.created < query["start"])
        
        statement = statement \
            .order_by(Delivery.created.desc()) \
            .limit(query["per_page"])

        rows = self.session.scalars(statement).all()
        
        # The next_token tells the client how to try to get the next page.
        if len(rows) == 0:
            self.next_token = None
        else:
            self.next_token = rows[-1].created
        
        for row in rows:
            self.feed.append(row.id)


    with Session() as session:
        builder = FeedBuilder(session)
        bind(builder, get_primary)

        builder.get_primary()
        builder.pull_deliveries()
        builder.pull_drafts()
        builder.pull_files()
        builder.pull_targets()

        # Collate it all for a response graph.
        graph = {
            "feed": builder.feed,
            "deliveries": builder.deliveries,
            "drafts": builder.drafts,
            "files": builder.files,
            "targets": builder.targets
        }

        if builder.next_token is not None:
            graph["next"] = builder.next_token

        return graph