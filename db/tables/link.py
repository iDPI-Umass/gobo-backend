import logging
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import handle_optional

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


    def to_dict(self):
        json = {
            "id": self.id,
            "created": self.created,
            "updated": self.updated
        }

        handle_optional(self, json, [
          "origin_type",
          "origin_id",
          "target_type",
          "target_id",
          "name"
        ])

        return json


    def update(self, json):
        self.origin_type = json["origin_type"]
        self.origin_id = json["origin_id"]
        self.target_type = json["target_type"]
        self.target_id = json["target_id"]
        self.name = json["name"]
        self.updated = joy.time.now()