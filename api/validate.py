import logging
from flask import Flask, request
from jsonschema import validate, ValidationError
import http_errors


def validate_request(configuration):
    schema = configuration.get("request").get("schema")
    if schema is None:
        return

    try:
        try:
            json = getattr(request, "json", {})
        except Exception as e:
            logging.warning(e)
            raise http_errors.bad_request("unable to parse body")

        validate(schema=schema, instance=json)
    except ValidationError as e:
        raise http_errors.bad_request(e.message)
  