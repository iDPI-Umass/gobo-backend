import logging
import json
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "identity_id",
    "post_id",
    "name",
    "secondary"
]

class PostEdge(Base):
    __tablename__ = "post_edge"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    identity_id: Mapped[Optional[int]]
    post_id: Mapped[Optional[int]]
    name: Mapped[Optional[str]]
    secondary: Mapped[Optional[str]]
    stash: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        _data = data.copy()

        stash = _data.get("stash", None)
        if stash is not None:
            _data["stash"] = json.dumps(stash)

        return PostEdge(**_data)

    def to_dict(self):
        data = {
            "id": self.id,
            "created": self.created,
            "updated": self.updated
        }

        stash = getattr(self, "stash", None)
        if stash is not None:
            data["stash"] = json.loads(stash)

        read_optional(self, data, optional)
        return data

    def update(self, data):
        stash = data.get("stash", None)
        if stash is not None:
            self.stash = json.dumps(stash)

        write_optional(self, data, optional)
        self.updated = joy.time.now()