from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("postgresql://GOBO:password@db:5432/GOBO")
Session = sessionmaker(bind=engine)

from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass