import logging
import threading
import queues
import jobs


class Thread():
    def __init__(self, queue, dispatch):
        def main():
            while True:
                try:
                    task = queue.get()
                    dispatch(task)
                    queue.task_done()
                except Exception as e:
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

def start_database(count):
    for i in range(count):
        thread = Thread(queues.database, jobs.database.dispatch)
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