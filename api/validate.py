import logging
from flask import Flask, request
from jsonschema import validate, ValidationError
import http_errors


def validate_request(configuration):
    schema = configuration.get("request").get("schema")
    if schema is None:
        return

    try:
        validate(schema=schema, instance=request.json)
    except ValidationError as e:
        raise http_errors.bad_request(e.message)
  