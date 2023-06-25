import logging
from sqlalchemy import select
from db import tables
from db.base import Session


def where(key, value, operator = "eq"):
    return {
        "key": key,
        "value": value,
        "operator": operator
    }


def define_crud(Table):
    def add(data):
        with Session() as session:
            row = Table.write(data)
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
            direction = data.get("direction") or "descending"
            view = data.get("view") or "created"

            if direction == "descending":
                attribute = getattr(Table, view).desc()
            else:
                attribute = getattr(Table, view)

            if data["page"] == 1:
                offset = None
            else:
                offset = (data["page"] - 1) * data["per_page"]



            statement = select(Table)

            for expression in data["where"]:
                key = expression["key"]
                value = expression["value"]
                if expression["operator"] == "eq":
                    statement = statement.where(getattr(Table, key) == value)
                if expression["operator"] == "neq":
                    statement = statement.where(getattr(Table, key) != value)
                elif expression["operator"] == "gte":
                    statement = statement.where(getattr(Table, key) >= value)
                elif expression["operator"] == "gt":
                    statement = statement.where(getattr(Table, key) > value)
                elif expression["operator"] == "lte":
                    statement = statement.where(getattr(Table, key) <= value)
                elif expression["operator"] == "lt":
                    statement = statement.where(getattr(Table, key) < value)
                elif expression["operator"] == "in":
                    statement = statement.where(getattr(Table, key).in_(value))

            statement = statement.order_by(attribute) \
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

    def pull(where, _data = None):
        data = _data or {}
        data.setdefault("page", 1)
        data.setdefault("per_page", 500)
        data["where"] = where

        results = []
        while True:
            _results = query(data)
            results.extend(_results)
            if len(_results) != data["per_page"]:
                break
            else:
                data["page"] = data["page"] + 1

        return results


    def pluck(ids):
        with Session() as session:
            statement = select(Table).where(Table.id.in_(ids))
            rows = session.scalars(statement).all()
            results = []
            for row in rows:
                results.append(row.to_dict())

            return results


    return {
      "add": add,
      "get": get,
      "update": update,
      "remove": remove,
      "query": query,
      "find": find,
      "pull": pull,
      "pluck": pluck
    }