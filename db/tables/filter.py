from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import handle_optional


class Filter(Base):
    __tablename__ = "filter"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int]
    word: Mapped[Optional[str]]
    category: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    
    def to_dict(self):
        json = {
            "id": self.id,
            "person_id": self.person_id,
            "created": self.created,
            "updated": self.updated
        }

        handle_optional(self, json, ["word", "category"])
        return json

    def update(self, json):
        self.person_id = json["person_id"]
        self.word = json["word"]
        self.category = json["category"]
        self.updated = joy.time.now()