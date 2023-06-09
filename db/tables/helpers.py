import logging

def read_optional (row, json, fields):
    for field in fields:
        value = getattr(row, field)
        if value != None:
            json[field] = value

def write_optional(row, json, fields):
    for field in fields:
        setattr(row, field, json.get(field))
   