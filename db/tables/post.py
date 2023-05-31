from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base


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

    
    def to_dict(self):
        return {
            "id": self.id,
            "source_id": self.source_id,
            "base_url": self.base_url,
            "platform_id": self.platform_id,
            "title": self.title,
            "content": self.content,
            "author": self.author,
            "url": self.url,
            "visibility": self.visibility,
            "created": self.created,
            "updated": self.updated
        }

    def update(self, json):
        self.source_id = json["source_id"]
        self.base_url = json["base_url"]
        self.platform_id = json["platform_id"]
        self.title = json["title"]
        self.content = json["content"]
        self.author = json["author"]
        self.url = json["url"]
        self.visibility = json["visibility"]
        self.updated = joy.time.now()