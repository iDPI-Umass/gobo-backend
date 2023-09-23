import logging
import threading
import queues
import jobs
import joy

class Thread():
    def __init__(self, queue, dispatch):
        def main():
            while True:
                try:
                    task = queue.get()
                    created = joy.time.convert("iso", "date", task.created)
                    logging.info(f"starting {queue.name} {task.name} {task.id} latency: {joy.time.latency(created)}")
                    
                    stop_timer = joy.time.timer()
                    response = dispatch(task)
                    logging.info(f"finished {queue.name} {task.name} {task.id} duration: {stop_timer()}")
                    
                    task.progress(queues, response)
                    queue.task_done()
                
                except joy.error.RecoverableException as e:
                    logging.error(f"trying to recover from failure in {queue.name} {task.name}")
                    logging.warning(e, exc_info=True)
                    task.tries = task.tries + 1
                    if task.tries < 3:
                        queue.put_task(task)
                    else:
                        # TODO: Create dead-letter queue.
                        task.remove()
                
                except Exception as e:
                    logging.error(f"failure in {queue.name} {task.name}")
                    logging.error(e, exc_info=True)
                    if task != None:
                        queue.task_done()
                        task.tries = task.tries + 1
                        if task.tries < 0:
                            queue.put_task(task)
                        else:
                            # TODO: Create dead-letter queue.
                            task.remove()
                
        
        self.thread = threading.Thread(target=main)

    def start(self):
        self.thread.start()



def start_api():  
    thread = Thread(queues.api, jobs.api.dispatch)
    thread.start()

def start_test(count):
    for i in range(count):
        thread = Thread(queues.test, jobs.test.dispatch)
        thread.start()

def start_default(count):
    for i in range(count):
        thread = Thread(queues.default, jobs.default.dispatch)
        thread.start()

def start_bluesky(count):
    for i in range(count):
        thread = Thread(queues.bluesky, jobs.bluesky.dispatch)
        thread.start()

def start_reddit(count):
    for i in range(count):
        thread = Thread(queues.reddit, jobs.reddit.dispatch)
        thread.start()

def start_mastodon(count):
    for i in range(count):
        thread = Thread(queues.mastodon, jobs.mastodon.dispatch)
        thread.start()