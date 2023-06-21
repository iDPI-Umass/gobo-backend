# Load configuration
from dotenv import load_dotenv
load_dotenv()


# Configure GOBO logging
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
    level=logging.INFO
)


# Establish worker threads that drive its work.
import time
from sources import schedule, start_sources
import threads

threads.start_api()
threads.start_test(1)
threads.start_database(1)
threads.start_twitter(1)
threads.start_reddit(1)
# threads.start_mastodon(1)
start_sources()


logging.info("GOBO worker is online")

# Main loop that keeps the worker online and issuing cron events into queues.
while 1:
    schedule.run_pending()
    time.sleep(1)