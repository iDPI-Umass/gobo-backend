import joy
import models
import queue


class Task():
    def __init__(self, queue, name, details = None, id = None, tries = 0):
        self.id = id or joy.crypto.random({"encoding": "safe-base64"})
        self.queue = queue
        self.handler = None
        self.name = name
        self.tries = tries
        self.details = details or {}
        
        now = joy.time.now()
        self.created = now
        self.updated = now

    def __repr__(self): 
        details = {}
        for key, value in self.details.items():
            details[key] = value

        return str({
          "id": self.id,
          "queue": self.queue,
          "name": self.name,
          "tries": self.tries,
          "handler": self.handler,
          "created": self.created,
          "updated": self.updated,
        })
    
    def __str__(self): 
        return self.__repr__()


    def update(self, data):
        for key, value in data.items():
            self.details[key] = value
        self.updated = joy.time.now()

    def remove(self):
        models.task.remove(self.id)


class Queue():
    def __init__(self, name):
        self.name = name
        self.queue = queue.Queue()

    def put_task(self, task):
        self.queue.put(task)

    def put_details(self, name, details = None):
        task = Task(
            queue = self.name,
            name = name,
            details = details
        )

        self.queue.put(task)

    def put_dict(self, task):
        task = Task(
          id = task["id"],
          queue = task["queue"],
          name = task["name"],
          details = task["details"]
        )

        self.queue.put(task)

    def get(self):
        return self.queue.get()

    def task_done(self):
        return self.queue.task_done()


api = Queue("api")
test = Queue("test")
twitter = Queue("twitter")
reddit = Queue("reddit")
mastodon = Queue("mastodon")
database = Queue("database")