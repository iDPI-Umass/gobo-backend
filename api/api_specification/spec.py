from yaml import safe_load

spec = None
with open("api_specification/spec.yaml", "r") as file:
    spec = safe_load(file.read())