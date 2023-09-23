import joy
import models
import queue


class Task():
    def __init__(self, queue, name, details = None, id = None, tries = 0, flow = []):
        self.id = id or joy.crypto.random({"encoding": "safe-base64"})
        self.queue = queue
        self.name = name
        self.reset_tracking()
        self.details = details or {}

        self.flow = flow

    def __repr__(self): 
        details = {}
        for key, value in self.details.items():
            details[key] = value

        return str({
          "id": self.id,
          "queue": self.queue,
          "name": self.name,
          "tries": self.tries,
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
        if type(self.id) == int:
            models.task.remove(self.id)

    def reset_tracking(self):
        now = joy.time.now()
        self.created = now
        self.updated = now
        self.tries = 0

    def halt(self):
        self.is_halted = True

    def progress(self, queues, response = {}):
        if self.is_halted == True:
            return

        next = None
        if len(self.flow) > 0:
            next = self.flow.pop(0)
        
        if next is not None:
            queue = getattr(queues, next["queue"])
            self.queue = queue.name
            self.name = next["name"]
            self.reset_tracking()
            
            for key, value in response.items():
                self.details[key] = value

            next_details = next.get("details", {})
            for key, value in next_details.items():
                self.details[key] = value

            queue.put_task(self)


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

    def put_flow(self, flow):
        self.put_task(Task(
            queue = self.name,
            name = "start flow",
            flow = flow
        ))

    def get(self):
        return self.queue.get()

    def task_done(self):
        return self.queue.task_done()


api = Queue("api")
test = Queue("test")
default = Queue("default")
bluesky = Queue("bluesky")
reddit = Queue("reddit")
mastodon = Queue("mastodon")