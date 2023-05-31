from typing import Optional
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base


class Identity(Base):
    __tablename__ = "identity"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int]
    base_url: Mapped[Optional[str]]
    profile_url: Mapped[Optional[str]]
    profile_image: Mapped[Optional[str]]
    username: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    oauth_token: Mapped[Optional[str]]
    oauth_token_secret: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    
    def to_dict(self):
        return {
            "id": self.id,
            "person_id": self.person_id,
            "base_url": self.base_url,
            "profile_url": self.profile_url,
            "profile_image": self.profile_image,
            "username": self.username,
            "name": self.name,
            "oauth_token": self.oauth_token,
            "oauth_token_secret": self.oauth_token_secret,
            "created": self.created,
            "updated": self.updated
        }

    def update(self, json):
        self.person_id = json["person_id"]
        self.base_url = json["base_url"]
        self.profile_url = json["profile_url"]
        self.profile_image = json["profile_image"]
        self.username = json["username"]
        self.name = json["name"]
        self.oauth_token = json["oauth_token"]
        self.oauth_token_secret = json["oauth_token_secret"]
        self.updated = joy.time.now()