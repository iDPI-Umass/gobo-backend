from base import engine, Base

print("Reading table model declarations...")

import person
import identity

print("Applying create all...")

Base.metadata.create_all(engine)

print("Metadata setup complete.")