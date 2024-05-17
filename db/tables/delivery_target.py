import json
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "state"
]

class DeliveryTarget(Base):
    __tablename__ = "delivery_target"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int]
    identity_id: Mapped[int]
    delivery_id: Mapped[int]
    state: Mapped[Optional[str]]
    stash: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        _data = data.copy()

        stash = _data.get("stash")
        if stash is not None:
            _data["stash"] = json.dumps(stash)

        return DeliveryTarget(**_data)

    def to_dict(self):
        data = {
            "id": self.id,
            "person_id": self.person_id,
            "identity_id": self.identity_id,
            "delivery_id": self.delivery_id,
            "created": self.created,
            "updated": self.updated
        }

        stash = getattr(self, "stash", None)
        if stash is not None:
            data["stash"] = json.loads(stash)

        read_optional(self, data, optional)

        return data

    def update(self, data):
        self.person_id = data["person_id"]
        self.identity_id = data["identity_id"]
        self.delivery_id = data["delivery_id"]
        write_optional(self, data, optional)

        stash = data.get("stash")
        if stash is not None:
            self.stash = json.dumps(stash)

        self.updated = joy.time.now()