from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "base_url",
    "client_id",
    "client_secret"
]

class MastodonClient(Base):
    __tablename__ = "mastodon_client"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    base_url: Mapped[Optional[str]]
    client_id: Mapped[Optional[str]]
    client_secret: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        return MastodonClient(**data)
    
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