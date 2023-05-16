from typing import Optional
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
import joy
from ..base import Base


class Identity(Base):
    __tablename__ = "identity"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int] = mapped_column(ForeignKey("person.id"))
    base_url: Mapped[Optional[str]]
    profile_url: Mapped[Optional[str]]
    profile_image: Mapped[Optional[str]]
    username: Mapped[Optional[str]]
    name: Mapped[Optional[str]]
    oauth_token: Mapped[Optional[str]]
    oauth_token_secret: Mapped[Optional[str]]
    created: Mapped[str] = mapped_column(insert_default=joy.time.now())
    updated: Mapped[str] = mapped_column(insert_default=joy.time.now())