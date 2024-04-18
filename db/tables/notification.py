import logging
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
    "type",
    "notified",
    "source_id",
    "post_id",
    "active"
]

class Notification(Base):
    __tablename__ = "notification"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform: Mapped[Optional[str]]
    platform_id: Mapped[str]
    base_url: Mapped[Optional[str]]
    type: Mapped[Optional[str]]
    notified: Mapped[Optional[str]]
    source_id: Mapped[Optional[str]]
    post_id: Mapped[Optional[str]]
    post_meta: Mapped[Optional[str]]
    active: Mapped[bool] = mapped_column(insert_default=False)
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)


    @staticmethod
    def write(data):
        _data = data.copy()

        post_meta = _data.get("post_meta")
        if post_meta is not None:
            _data["post_meta"] = json.dumps(post_meta)

        return Notification(**_data)
    
    def to_dict(self):
        data = {
            "id": self.id,
            "platform_id": self.platform_id,
            "created": self.created,
            "updated": self.updated
        }

        post_meta = getattr(self, "post_meta", None)
        if post_meta is not None:
            data["post_meta"] = json.loads(post_meta)

        read_optional(self, data, optional)
        return data


    def update(self, data):
        self.platform_id = data["platform_id"]
        
        post_meta = data.get("post_meta")
        if post_meta is not None:
            self.post_meta = json.dumps(post_meta)
        
        write_optional(self, data, optional)
        self.updated = joy.time.now()