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
        "file_trace": {
            "class": "logging.FileHandler",
            "filename": "gobo.log",
            "formatter": "default"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["stdout", "file_trace"]
    }
})


# Establish worker threads that drive its work.
import time
from sources import schedule, start_sources
import threads

threads.start_api()
threads.start_test(1)
threads.start_database(2)
# threads.start_twitter(1)
threads.start_reddit(1)
threads.start_mastodon(1)
start_sources()


logging.info("GOBO worker is online")

# Main loop that keeps the worker online and issuing cron events into queues.
while 1:
    schedule.run_pending()
    time.sleep(1)