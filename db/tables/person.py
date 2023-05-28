import logging
from typing import Optional
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base


class Person(Base):
    __tablename__ = "person"

    id: Mapped[str] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now())
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now())


    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "created": self.created,
            "updated": self.updated
        }

    def update(self, json):
        self.name = json["name"]
        self.updated = joy.time.now()