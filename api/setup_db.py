from db.base import engine, Base

print("Reading table model declarations...")

from db import tables

print("Applying create all...")

Base.metadata.create_all(engine)

print("Metadata setup complete.")