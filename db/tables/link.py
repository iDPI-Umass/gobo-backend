import logging
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "origin_type",
    "origin_id",
    "target_type",
    "target_id",
    "name"
]

class Link(Base):
    __tablename__ = "link"

    id: Mapped[str] = mapped_column(Integer, primary_key=True)
    origin_type: Mapped[Optional[str]]
    origin_id: Mapped[Optional[int]]
    target_type: Mapped[Optional[str]]
    target_id: Mapped[Optional[int]]
    name: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        return Link(**data)

    def to_dict(self):
        data = {
            "id": self.id,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, data, optional)
        return data

    def update(self, data):
        write_optional(self, data, optional)
        self.updated = joy.time.now()