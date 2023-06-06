def handle_optional (row, json, fields):
    for field in fields:
        value = getattr(row, field)
        if value != None:
            json[field] = value
   