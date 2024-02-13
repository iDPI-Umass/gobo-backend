import logging
import json
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "queue",
    "name",
    "priority",
]

class Task(Base):
    __tablename__ = "task"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    queue: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    priority: Mapped[Optional[int]]
    details: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)


    @staticmethod
    def write(data):
        _data = data.copy()
        details = _data.get("details", {})
        _data["details"] = json.dumps(details)
        return Task(**_data)

    def to_dict(self):
        data = {
            "id": self.id,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, data, optional)
        details = getattr(self, "details", {})
        data["details"] = json.loads(details)
        
        return data

    def update(self, data):
        write_optional(self, data, optional)
        details = data.get("details") or {}
        self.details = json.dumps(details)
        self.updated = joy.time.now()