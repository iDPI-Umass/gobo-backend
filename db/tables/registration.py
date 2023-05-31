from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base


class Registration(Base):
    __tablename__ = "registration"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int]
    base_url: Mapped[Optional[str]]
    oauth_token: Mapped[Optional[str]]
    oauth_token_secret: Mapped[Optional[str]]
    saved_state: Mapped[Optional[str]]
    request_time: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    
    def to_dict(self):
        return {
            "id": self.id,
            "person_id": self.person_id,
            "base_url": self.base_url,
            "oauth_token": self.oauth_token,
            "oauth_token_secret": self.oauth_token_secret,
            "saved_state": self.saved_state,
            "request_time": self.request_time,
            "created": self.created,
            "updated": self.updated
        }

    def update(self, json):
        self.person_id = json["person_id"]
        self.base_url = json["base_url"]
        self.oauth_token = json["oauth_token"]
        self.oauth_token_secret = json["oauth_token_secret"]
        self.saved_state = json["saved_state"]
        self.request_time = json["request_time"]
        self.updated = joy.time.now()