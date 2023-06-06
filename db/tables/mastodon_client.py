from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import handle_optional


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

        handle_optional(self, json, [
          "base_url",
          "client_id",
          "client_secret"
        ])

        return json

    def update(self, json):
        self.base_url = json["base_url"]
        self.client_id = json["client_id"]
        self.client_secret = json["client_secret"]
        self.updated = joy.time.now()