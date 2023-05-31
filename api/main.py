# Configure GOBO logging
from logging import config
import logging
config.dictConfig({
    "version": 1,
    "formatters": {"default": {
      "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
    }},
    "handlers": {"wsgi": {
        "class": "logging.StreamHandler",
        "stream": "ext://flask.logging.wsgi_errors_stream",
        "formatter": "default"
    }},
    "root": {
        "level": "INFO",
        "handlers": ["wsgi"]
    }
})


# Instntiate Flask app
from flask import Flask, request
app = Flask(__name__)


# Establish handlers based on API specification
from jsonschema import validate, ValidationError
from api_specification import spec
from http_errors import HTTPError
import handlers

def wrap_handler(alias, configuration, handler):

    def validateSchema(*args, **kwargs):
        schema = configuration.get("request").get("schema")
        if schema is None:
            return

        validate(schema=schema, instance=request.json)
        

    def f(*args, **kwargs):
        try:
            validateSchema(args, kwargs)
        except ValidationError as e:
            response = {"validation_error": e.message}
            logging.warning(response)
            return response, 400


        try:
            status = configuration["response"]["status"]
            result = handler(*args, **kwargs)
            return result, status
        except (Exception, HTTPError) as e:
            status = getattr(e, "status", 500)
            if status == 500:
                # Log this as an unhandled error and provide limited data to client.
                logging.error(e)
                result = {}
            else:
                # Log this as a handled error and provide relevant data to client.
                logging.warning(e)
                result = {"message": e.message}
          
            return result, status
    
    # Flask needs unique function names internally.
    # Rename the wrapper function to match the inner function.
    f.__name__ = alias
    return f


for name, resource in spec["resources"].items():
    for method, configuration in resource["methods"].items():
        key = f"{name}_{method}"
        handler = getattr(handlers, key)
        wrapped = wrap_handler(key, configuration, handler)
        decorate = app.route(resource["route"], methods=[method])
        decorate(wrapped)


# Startup Flask server
app.run(host="0.0.0.0", debug=True)