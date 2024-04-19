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
            "backupCount": 10
        }
    },
   "root": {
        "level": "INFO",
        "handlers": ["stdout", "main_trace", "error_trace"]
    }
})


# Establish worker queues and threads that drive its work.
from os import environ
import time
from cron import schedule, start_sources
import threads
import queues

counts = {
    "default": int(environ.get("DEFAULT_THREAD_COUNT", 1)),
    "bluesky": int(environ.get("BLUESKY_THREAD_COUNT", 1)),
    "mastodon": int(environ.get("MASTODON_THREAD_COUNT", 1)),
    "reddit": int(environ.get("REDDIT_THREAD_COUNT", 1)),
    "smalltown": int(environ.get("SMALLTOWN_THREAD_COUNT", 1))
}


queues.build_sharded_queues(counts)
threads.set_thread_counts(counts)

threads.start_api()
threads.start_default()
threads.start_bluesky()
threads.start_mastodon()
threads.start_reddit()
threads.start_smalltown()

start_sources()


logging.info("GOBO worker is online")

# Main loop that keeps the worker online and issuing cron events into queues.
while True:
    schedule.run_pending()
    time.sleep(1)