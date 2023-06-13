import queue
import joy


class Task():
    def __init__(self, queue, name, details = None):
        self.queue = queue
        self.handler = None
        self.name = name
        self.details = details or {}
        
        now = joy.time.now()
        self.created = now
        self.update = now

    def __repr__(self): 
        details = {}
        for key, value in self.details.items():
            details[key] = value
        data["details"] = details

        return {
          "queue": self.queue,
          "name": self.name,
          "handler": self.handler,
          "created": self.created,
          "updated": self.updated,
          "details": details
        }
    
    def __str__(self): 
        return self.__repr__()


    def update(self, data):
        for key, value in data.items():
            self.details[key] = value
        self.updated = joy.time.now()


class Queue():
    def __init__(self, name):
        self.name = name
        self.queue = queue.Queue()

    def put(self, name, details = None):
        task = Task(
            queue = self.queue,
            name = name,
            details = details
        )

        self.queue.put(task)


twitter = Queue("twitter")
reddit = Queue("reddit")
mastodon = Queue("mastodon")