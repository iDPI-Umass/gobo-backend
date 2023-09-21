import json
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "base_url",
    "platform",
    "platform_id",
    "title",
    "content",
    "url",
    "visibility",
    "published"
]

class Post(Base):
    __tablename__ = "post"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int]
    base_url: Mapped[Optional[str]]
    platform: Mapped[Optional[str]]
    platform_id: Mapped[Optional[str]]
    title: Mapped[Optional[str]]
    content: Mapped[Optional[str]]
    url: Mapped[Optional[str]]
    visibility: Mapped[Optional[str]]
    published: Mapped[Optional[str]]
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
        
        return Post(**_data)
    
    def to_dict(self):
        data = {
            "id": self.id,
            "source_id": self.source_id,
            "created": self.created,
            "updated": self.updated
        }

        attachments = getattr(self, "attachments")
        if attachments != None:
            data["attachments"] = json.loads(attachments)

        poll = getattr(self, "poll")
        if poll != None:
            data["poll"] = json.loads(poll)

        read_optional(self, data, optional)
        return data

    def update(self, data):
        self.source_id = data["source_id"]
        write_optional(self, data, optional)

        attachments = data.get("attachments")
        if attachments != None:
            self.attachments = json.dumps(attachments)

        poll = data.get("poll")
        if poll != None:
            self.poll = json.dumps(poll)

        self.updated = joy.time.now()