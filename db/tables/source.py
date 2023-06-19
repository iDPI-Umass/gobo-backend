from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "base_url",
    "url",
    "username",
    "name",
    "icon_url",
    "active"
]

class Source(Base):
    __tablename__ = "source"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_id: Mapped[str]
    base_url: Mapped[Optional[str]]
    url: Mapped[Optional[str]]
    username: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    icon_url: Mapped[Optional[str]]
    active: Mapped[bool] = mapped_column(insert_default=False)
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)


    @staticmethod
    def write(data):
        return Source(**data)
    
    def to_dict(self):
        data = {
            "id": self.id,
            "platform_id": self.platform_id,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, data, optional)
        return data


    def update(self, data):
        self.platform_id = data["platform_id"]
        write_optional(self, data, optional)
        self.updated = joy.time.now()