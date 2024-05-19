import logging
from typing import Optional
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "name",
    "filename",
    "size",
    "alt",
    "mime_type",
    "state"
]

class DraftFile(Base):
    __tablename__ = "draft_file"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    person_id: Mapped[int]
    name: Mapped[Optional[str]]
    filename: Mapped[Optional[str]]
    size: Mapped[Optional[int]]
    alt: Mapped[Optional[str]]
    state: Mapped[Optional[str]]
    published: Mapped[bool] = mapped_column(insert_default=False)
    mime_type: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        return DraftFile(**data)

    def to_dict(self):
        data = {
            "id": self.id,
            "person_id": self.person_id,
            "published": self.published,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, data, optional)
        return data

    def update(self, data):
        write_optional(self, data, optional)
        self.person_id = data["person_id"]
        self.published = data["published"]
        self.updated = joy.time.now()