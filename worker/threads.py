import logging
import threading
import queues
import jobs
import joy

# TODO: Consider recovery and dead-letter states.
def fail_task(queue, task, error):
    try:
        if task is None:
            # This should never happen, so it's alarming if it does.
            logging.error("exception in task processing, but no task defined")
            return
        
        logging.error(f"failure in {queue.name} {task.name}")
        logging.error(error, exc_info=True)
        task.failure()
        task.remove()
    

    # This is the failsafe designed to catch any issues with the above, more
    # sophisticated error handling and keeps the thread alive for next task.
    # It's like a 500-class HTTP error, so it should ideally not get here.
    # Because the first priority is to keep the thread alive, this handler
    # needs to be guaranteed to always resolve without another exception.
    except Exception as abject:
        logging.error("abject thread failure")
        logging.error(abject, exc_info=True)




def thread_core(queue, dispatch):
    while True:
        try:
            task = queue.get()
            task.start(queue)
            result = dispatch(task)
            task.finish(queue)
            task.progress(queues, result)
            task.remove()
        except Exception as e:
            fail_task(queue, task, e)

        queue.task_done()
 
        
class Thread():
    def __init__(self, queue, dispatch):  
        self.thread = threading.Thread(
            target = thread_core,
            args = (queue, dispatch)
        )

    def start(self):
        self.thread.start()


thread_counts = {}
def set_thread_counts(counts):
    thread_counts.update(counts)



def start_api():  
    thread = Thread(queues.api, jobs.api.dispatch)
    thread.start()

def start_default():
    for i in range(thread_counts["default"]):
        thread = Thread(queues.default, jobs.default.dispatch)
        thread.start()

def start_bluesky():
    for i in range(thread_counts["bluesky"]):
        thread = Thread(queues.bluesky[i], jobs.bluesky.dispatch)
        thread.start()

def start_linkedin():
    for i in range(thread_counts["linkedin"]):
        thread = Thread(queues.linkedin[i], jobs.linkedin.dispatch)
        thread.start()

def start_mastodon():
    for i in range(thread_counts["mastodon"]):
        thread = Thread(queues.mastodon[i], jobs.mastodon.dispatch)
        thread.start()

def start_reddit():
    for i in range(thread_counts["reddit"]):
        thread = Thread(queues.reddit[i], jobs.reddit.dispatch)
        thread.start()

def start_smalltown():
    for i in range(thread_counts["smalltown"]):
        thread = Thread(queues.smalltown[i], jobs.smalltown.dispatch)
        thread.start()