import logging
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
   "name"
]

class Person(Base):
    __tablename__ = "person"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    authority_id: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)


    def to_dict(self):
        json = {
            "id": self.id,
            "authority_id": self.authority_id,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, json, optional)
        return json
          

    def update(self, json):
        self.authority_id = json["authority_id"]
        write_optional(self, json, optional)
        self.updated = joy.time.now()