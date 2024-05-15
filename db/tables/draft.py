import logging
import json
from typing import Optional
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "title",
    "content",
    "state"
]


class Draft(Base):
    __tablename__ = "draft"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    person_id: Mapped[int]
    state: Mapped[Optional[str]]
    title: Mapped[Optional[str]]
    content: Mapped[Optional[str]]
    attachments: Mapped[Optional[str]]
    poll: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        _data = data.copy()
        
        attachments = _data.get("attachments", None)
        if attachments is not None:
            _data["attachments"] = json.dumps(attachments)

        poll = _data.get("poll", None)
        if poll is not None:
            _data["poll"] = json.dumps(poll)
        
        return Draft(**_data)

    def to_dict(self):
        data = {
            "id": self.id,
            "person_id": self.person_id,
            "created": self.created,
            "updated": self.updated
        }

        attachments = getattr(self, "attachments", None)
        if attachments is not None:
            data["attachments"] = json.loads(attachments)

        poll = getattr(self, "poll", None)
        if poll is not None:
            data["poll"] = json.loads(poll)

        read_optional(self, data, optional)
        return data

    def update(self, data):
        write_optional(self, data, optional)
        attachments = data.get("attachments")
        if attachments != None:
            self.attachments = json.dumps(attachments)

        poll = data.get("poll")
        if poll != None:
            self.poll = json.dumps(poll)

        self.updated = joy.time.now()