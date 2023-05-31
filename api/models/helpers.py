import logging
from sqlalchemy import select
from db import tables
from db.base import Session

def define_crud(Table):
    def add(data):
        with Session() as session:
            row = Table(**data)
            session.add(row)
            session.commit()
            return row.to_dict()

    def get(id):
        with Session() as session:
            row = session.get(Table, id)
            if row == None:
                return None
            else:
                return row.to_dict()

    def update(id, data):
        with Session() as session:
            row = session.get(Table, id)
            if row == None:
                return None
            else:
                row.update(data)
                session.commit()
                return row.to_dict()

    def remove(id):
        with Session() as session:
            row = session.get(Table, id)
            if row == None:
                return None
            else:
                session.delete(row)
                session.commit()
                return row.to_dict()

    def query(data):
        with Session() as session:  
            if data["direction"] == "descending":
                attribute = getattr(Table, data["view"]).desc()
            else:
                attribute = getattr(Table, data["view"])

            if data["page"] == 1:
                offset = None
            else:
                offset = (data["page"] - 1) * data["per_page"]

            for expression in data["where"]:
                key = expression["key"]
                value = expression["value"]
                if expression["operator"] == "eq":
                    statement.where(getattr(Table, key) == value)
                elif expression["operator"] == "gte":
                    statement.where(getattr(Table, key) >= value)
                elif expression["operator"] == "gt":
                    statement.where(getattr(Table, key) > value)
                elif expression["operator"] == "lte":
                    statement.where(getattr(Table, key) <= value)
                elif expression["operator"] == "lt":
                    statement.where(getattr(Table, key) < value)

            statement = select(Table) \
                        .order_by(attribute) \
                        .offset(offset) \
                        .limit(data["per_page"])

            rows = session.scalars(statement).all()

            results = []
            for row in rows:
                results.append(row.to_dict())
            return results

    def find(data):
        with Session() as session:
            statement = select(Table)
            for key, value in data.items():
                statement = statement.where(getattr(Table, key) == value)
            statement = statement.limit(1)

            row = session.scalars(statement).first()

            if row == None:
                return None
            else:
                return row.to_dict()


    return {
      "add": add,
      "get": get,
      "update": update,
      "remove": remove,
      "query": query,
      "find": find
    }