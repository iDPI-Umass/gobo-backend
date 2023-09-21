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
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
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
        "handlers": ["stdout", "main_trace", "error_trace"]
    }
})


# Establish worker threads that drive its work.
import time
from sources import schedule, start_sources
import threads

threads.start_api()
threads.start_test(1)
threads.start_default(4)
threads.start_bluesky(1)
threads.start_reddit(1)
threads.start_mastodon(1)
start_sources()


logging.info("GOBO worker is online")

# Main loop that keeps the worker online and issuing cron events into queues.
while 1:
    schedule.run_pending()
    time.sleep(1)