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
    "name",
    "secondary"
]

class Counter(Base):
    __tablename__ = "counter"

    id: Mapped[str] = mapped_column(Integer, primary_key=True)
    origin_type: Mapped[Optional[str]]
    origin_id: Mapped[Optional[int]]
    target_type: Mapped[Optional[str]]
    target_id: Mapped[Optional[int]]
    name: Mapped[Optional[str]]
    secondary: Mapped[int] = mapped_column(insert_default=0)
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        return Counter(**data)

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

    def get_secondary(self):
        return getattr(self, "secondary", None)
    
    def set_secondary(self, value):
        self.secondary = value
        self.updated = joy.time.now()