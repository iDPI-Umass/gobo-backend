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

    
    def to_dict(self):
        json = {
            "id": self.id,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, json, optional)

        return json

    def update(self, json):
        write_optional(self, json, optional)
        self.updated = joy.time.now()