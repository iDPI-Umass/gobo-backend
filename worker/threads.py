import logging
import threading
import queues
import jobs
import joy

class MiniThread():
    def __init__(self, queue, dispatch):
        def main():
            while True:
                try:
                    task = queue.get()
                    dispatch(task)
                    queue.task_done() 
                except Exception as e:
                    logging.error(f"failure in {queue.name} {task.name}")
                    logging.error(e, exc_info=True)
                    if task != None:
                        queue.task_done()
                        # TODO: Create dead-letter queue.
                        task.remove()
      

        self.thread = threading.Thread(target=main)

    def start(self):
        self.thread.start()


class Thread():
    def __init__(self, queue, dispatch):
        def main():
            while True:
                try:
                    task = queue.get()
                    task.start(queue)
                    result = dispatch(task)
                    task.finish(queue)
                    task.progress(queues, result)
                    task.remove()
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
                        # TODO: Create dead-letter queue.
                        task.remove()
                
        
        self.thread = threading.Thread(target=main)

    def start(self):
        self.thread.start()



def start_api():  
    thread = Thread(queues.api, jobs.api.dispatch)
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

def start_smalltown(count):
    for i in range(count):
        thread = Thread(queues.smalltown, jobs.smalltown.dispatch)
        thread.start()


def start_mastodon(count):
    for i in range(count):
        thread = MiniThread(queues.mastodon, jobs.mastodon.super_dispatch)
        thread.start()  

def start_mastodon_default(count):
    for i in range(count):
        thread = Thread(queues.mastodon_default, jobs.mastodon.dispatch)
        thread.start()

def start_mastodon_social(count):
    for i in range(count):
        thread = Thread(queues.mastodon_social, jobs.mastodon.dispatch)
        thread.start()

def start_mastodon_hachyderm(count):
    for i in range(count):
        thread = Thread(queues.mastodon_hachyderm, jobs.mastodon.dispatch)
        thread.start()

def start_mastodon_octodon(count):
    for i in range(count):
        thread = Thread(queues.mastodon_octodon, jobs.mastodon.dispatch)
        thread.start()

def start_mastodon_techpolicy(count):
    for i in range(count):
        thread = Thread(queues.mastodon_techpolicy, jobs.mastodon.dispatch)
        thread.start()

def start_mastodon_vis_social(count):
    for i in range(count):
        thread = Thread(queues.mastodon_vis_social, jobs.mastodon.dispatch)
        thread.start()

def start_mastodon_social_coop(count):
    for i in range(count):
        thread = Thread(queues.mastodon_social_coop, jobs.mastodon.dispatch)
        thread.start()