import threading
import queues
import jobs


class Thread():
    def __init__(self, queue, dispatch):
        def main():
            while True:
                task = queue.get()
                dispatch(task)
                queue.task_done()
        
        self.thread = threading.Thread(target=main)

    def start(self):
        self.thread.start()



def start_twitter(count):
    for i in range(count):
        thread = Thread(queues.twitter, jobs.twitter.dispatch)
        thread.start()

def start_reddit(count):
    for i in range(count):
        thread = Thread(queues.reddit, jobs.reddit.dispatch)
        thread.start()

def start_mastodon(count):
    for i in range(count):
        thread = Thread(queues.mastodon, jobs.mastodon.dispatch)
        thread.start()