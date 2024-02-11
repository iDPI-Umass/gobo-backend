import logging
import threading
import queues
import jobs
import joy

# TODO: Consider recovery and dead-letter states.
def fail_task(queue, task, e):
    if task is None:
        # This should never happen, so it' alarming if it does.
        logging.error("exception in task processing, but no task defined")
        return
    
    logging.error(f"failure in {queue.name} {task.name}")
    logging.error(e, exc_info=True)
    task.failure()
    task.remove()
    queue.task_done()

def thread_core(queue, dispatch):
    while True:
        try:
            task = queue.get()
            task.start(queue)
            result = dispatch(task)
            task.finish(queue)
            task.progress(queues, result)
            task.remove()
            queue.task_done()
        except Exception as e:
            fail_task(queue, task, e)
 
        
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

def start_default(count):
    for i in range(thread_counts["default"]):
        thread = Thread(queues.default, jobs.default.dispatch)
        thread.start()

def start_bluesky(count):
    for i in range(thread_counts["bluesky"]):
        thread = Thread(queues.bluesky[i], jobs.bluesky.dispatch)
        thread.start()

def start_mastodon(count):
    for i in range(thread_counts["mastodon"]):
        thread = Thread(queues.mastodon[i], jobs.mastodon.dispatch)
        thread.start()

def start_reddit(count):
    for i in range(thread_counts["reddit"]):
        thread = Thread(queues.reddit[i], jobs.reddit.dispatch)
        thread.start()

def start_smalltown(count):
    for i in range(thread_counts["smalltown"]):
        thread = Thread(queues.smalltown[i], jobs.smalltown.dispatch)
        thread.start()