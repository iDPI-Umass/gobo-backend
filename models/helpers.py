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

def build_query(per_page, wheres):
    return {
        "page": 1,
        "per_page": per_page,
        "where": wheres
    }

class QueryIterator:
    def __init__(self, model, per_page = 1000, wheres = [], query = None, for_removal = False):
        self.model = model
        self.for_removal = for_removal
        self.feed = []
        self.state = "active"
        
        if query is not None:
            self.per_page = query["per_page"]
            self.query = query
        else:
            self.per_page = per_page
            self.query = build_query(per_page, wheres)

    def __iter__(self):
        return self
    
    def pull(self):
        if self.state == "done":
            return
        
        items = self.model.query(self.query)
        self.feed.extend(items)
        if len(items) != self.per_page:
            self.state = "done"
        elif self.for_removal == False:
            self.query["page"] += 1
    
    def __next__(self):
        if len(self.feed) == 0:
            self.pull()
            if len(self.feed) == 0:
                raise StopIteration
        
        return self.feed.pop(0)
    

class ViewIterator:
    def __init__(self, model, view, direction = "descending", per_page = 1000, wheres = []):
        self.model = model
        self.view = view
        self.feed = []
        self.state = "active"
        self.per_page = per_page
        self.wheres = wheres
        self.query = build_query(per_page, wheres)
        self.query["view"] = view
        self.query["direction"] = direction

    def __iter__(self):
        return self
    
    def pull(self):
        if self.state == "done":
            return
        
        items = self.model.query(self.query)
        self.feed.extend(items)
        if len(items) != self.per_page:
            self.state = "done"
        else:
            self.query["where"] = self.wheres
            value = items[-1].get(self.view, None)
            self.query["where"].append(where(self.view, value))
    
    def __next__(self):
        if len(self.feed) == 0:
            self.pull()
            if len(self.feed) == 0:
                raise StopIteration
        
        return self.feed.pop(0)



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
                elif expression["operator"] == "not in":
                    statement = statement.where(~getattr(Table, key).in_(value))

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

    def random(where_statements):
        with Session() as session:
            statement = select(Table)

            for expression in where_statements:
                key = expression["key"]
                value = expression["value"]
                if expression["operator"] == "eq":
                    statement = statement.where(getattr(Table, key) == value)
                elif expression["operator"] == "neq":
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
                else:
                    raise Exception(f"unknown where operator {expression['operator']}")
                    
            statement = statement.limit(1)
            row = session.scalars(statement).first()

            if row == None:
                return None
            else:
                return row.to_dict()
    
    def scan(data):
        with Session() as session:
            direction = data.get("direction") or "descending"
            per_page = data.get("per_page") or 1000

            if direction == "descending":
                attribute = Table.id.desc()
            else:
                attribute = Table.id


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
                elif expression["operator"] == "not in":
                    statement = statement.where(~getattr(Table, key).in_(value))

            statement = statement.order_by(attribute) \
                .limit(per_page)


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
      "pluck": pluck,
      "random": random,
      "scan": scan
    }