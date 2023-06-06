from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import handle_optional


class Source(Base):
    __tablename__ = "source"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    base_url: Mapped[Optional[str]]
    url: Mapped[Optional[str]]
    username: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    icon_url: Mapped[Optional[str]]
    active: Mapped[bool] = mapped_column(insert_default=False)
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    
    def to_dict(self):
        json = {
            "id": self.id,
            "base_url": self.base_url,
            "created": self.created,
            "updated": self.updated
        }

        handle_optional(self, json, [
          "url",
          "username",
          "name",
          "icon_url",
          "active"
        ])

        return json

    def update(self, json):
        self.base_url = json["base_url"]
        self.url = json["url"]
        self.username = json["username"]
        self.name = json["name"]
        self.icon_url = json["icon_url"]
        self.active = json["active"]
        self.updated = joy.time.now()