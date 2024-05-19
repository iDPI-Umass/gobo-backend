import logging
import json
from typing import Optional
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "state"
]


class Draft(Base):
    __tablename__ = "draft"

    id: Mapped[str] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int]
    state: Mapped[Optional[str]]
    store: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        _data = data.copy()
        
        store = _data.get("store")
        if store is not None:
            _data["store"] = json.dumps(store)
        
        return Draft(**_data)

    def to_dict(self):
        data = {
            "id": self.id,
            "person_id": self.person_id,
            "created": self.created,
            "updated": self.updated
        }

        store = getattr(self, "store", None)
        if store is not None:
            data["store"] = json.loads(store)

        read_optional(self, data, optional)
        return data

    def update(self, data):
        write_optional(self, data, optional)
      
        store = data.get("store")
        if store != None:
            self.store = json.dumps(store)

        self.updated = joy.time.now()