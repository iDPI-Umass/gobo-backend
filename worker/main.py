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
from cron_sources import schedule, start_sources
import threads

start_sources()
threads.start_twitter(1)
threads.start_reddit(1)
threads.start_mastodon(1)

# Main loop that keeps the worker online and checking the queue for task.
while 1:
    schedule.run_pending()
    time.sleep(1)