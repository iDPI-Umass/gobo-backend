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
    "handlers": {
        "wsgi": {
            "class": "logging.StreamHandler",
            "stream": "ext://flask.logging.wsgi_errors_stream",
            "formatter": "default"
        },
        "main_trace": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "gobo.log",
            "formatter": "default",
            "maxBytes": 10000000, # 10 MB
            "backupCount": 1
        },
        "error_trace": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "gobo-error.log",
            "level": "WARN",
            "formatter": "default",
            "maxBytes": 10000000, # 10 MB
            "backupCount": 1
        }
    },
   "root": {
          "level": "INFO",
          "handlers": ["wsgi", "main_trace", "error_trace"]
    }
})


# Instntiate Flask app
from time import process_time
import math
from flask import Flask, request, make_response
from flask_cors import CORS
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 4 * 1000 * 1000  # 4 MB upload limit
CORS(app)


# Establish handlers based on API specification
from api_specification import spec
from http_errors import HTTPError
from validate import validate_request
from authorize import authorize_request
import handlers

def log_duration(start, status):
    end = process_time()
    duration = math.floor((end - start) * 1000)
    logging.info(f"{status} {duration}ms {request.method} {request.full_path}")

def wrap_handler(alias, configuration, handler):

    def f(*args, **kwargs):
        start = process_time()
        response = None
        status = None

        try:            
            authorize_request(configuration)
            validate_request(configuration)
            result = handler(*args, **kwargs)
            status = configuration["response"]["status"]
            response = make_response(result, status)
            response.headers.add("Access-Control-Allow-Origin", "*")
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
        
        log_duration(start, status)
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


# Startup Flask server if in development
if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)