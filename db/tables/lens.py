import json
from typing import Optional
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base
from .helpers import read_optional, write_optional

optional = [
    "category"
]


class Lens(Base):
    __tablename__ = "lens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int]
    category: Mapped[Optional[str]]
    configuration: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now)
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now)

    @staticmethod
    def write(data):
        _data = data.copy()
        configuration = _data.get("configuration")
        if  configuration != None:
            _data["configuration"] = json.dumps(configuration)
        return Lens(**_data)

    def to_dict(self):
        data = {
            "id": self.id,
            "person_id": self.person_id,
            "created": self.created,
            "updated": self.updated
        }

        read_optional(self, data, optional)
        
        configuration = getattr(self, "configuration")
        if configuration != None:
            data["configuration"] = json.loads(configuration)
        
        return data

    def update(self, data):
        self.person_id = data["person_id"]
        write_optional(self, data, optional)

        configuration = data.get("configuration")
        if configuration != None:
            self.configuration = json.dumps(configuration)

        self.updated = joy.time.now()