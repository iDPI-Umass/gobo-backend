# Load configuration
from dotenv import load_dotenv
load_dotenv()


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
from flask import Flask, request, make_response
from flask_cors import CORS
app = Flask(__name__)
CORS(app)


# Establish handlers based on API specification
from api_specification import spec
from http_errors import HTTPError
from validate import validate_request
from authorize import authorize_request
import handlers


def wrap_handler(alias, configuration, handler):

    def f(*args, **kwargs):
        try:
            authorize_request(configuration)
            validate_request(configuration)
            result = handler(*args, **kwargs)
            status = configuration["response"]["status"]
            response = make_response(result, status)
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response
        except (Exception, HTTPError) as e:
            status = getattr(e, "status", 500)
            if status == 500:
                # Log this as an unhandled error and provide limited data to client.
                logging.error(e, exc_info=True)
                result = {}
            else:
                # Log this as a handled error and provide relevant data to client.
                logging.warning(e, exc_info=True)
                result = {"message": e.message}
          
            response = make_response(result, status)
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response
    
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