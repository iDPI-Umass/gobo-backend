from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "base_url",
    "platform_id",
    "title",
    "content",
    "author",
    "url",
    "visibility"
]

class Post(Base):
    __tablename__ = "post"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int]
    base_url: Mapped[Optional[str]]
    platform_id: Mapped[Optional[str]]
    title: Mapped[Optional[str]]
    content: Mapped[Optional[str]]
    author: Mapped[Optional[str]]
    url: Mapped[Optional[str]]
    visibility: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        return Post(**data)
    
    def to_dict(self):
        data = {
            "id": self.id,
            "source_id": self.source_id,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, data, optional)
        return data

    def update(self, data):
        self.source_id = data["source_id"]
        write_optional(self, data, optional)
        self.updated = joy.time.now()