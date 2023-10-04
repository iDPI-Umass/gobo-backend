import logging
import joy
import models
import queue


class Task():
    def __init__(self, name, details = None, id = None, tries = 0, flow = []):
        self.id = id or joy.crypto.random({"encoding": "safe-base64"})
        self.name = name
        self.reset_tracking()
        self.details = details or {}
        self.flow = flow

        self.is_quiet = False
        self.is_halted = False

    def __repr__(self): 
        details = {}
        for key, value in self.details.items():
            details[key] = value

        return str({
          "id": self.id,
          "name": self.name,
          "tries": self.tries,
          "created": self.created,
          "updated": self.updated,
        })
    
    def __str__(self): 
        return self.__repr__()
    

    def start(self, queue):
        created = joy.time.convert("iso", "date", self.created)
        if self.is_quiet != True:
            logging.info(f"starting {queue.name} {self.name} {self.id} latency: {joy.time.latency(created)}")
        
        self.start_time = joy.time.nowdate()

    def finish(self, queue):
        duration = joy.time.nowdate() - self.start_time
        if self.is_quiet != True:
            logging.info(f"finished {queue.name} {self.name} {self.id} duration: {duration}")

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

    def quiet(self):
        self.is_quiet = True

    def progress(self, queues, result = {}):
        result = result or {}
        if self.is_halted == True:
            return

        next = None
        if len(self.flow) > 0:
            next = self.flow.pop(0)
        
        if next is not None:
            queue = getattr(queues, next["queue"])
            self.name = next["name"]
            self.reset_tracking()
            
            for key, value in result.items():
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
            name = name,
            details = details
        )

        self.queue.put(task)

    def put_dict(self, task):
        task = Task(
          id = task["id"],
          name = task["name"],
          details = task["details"]
        )

        self.queue.put(task)

    def put_flow(self, flow):
        self.put_task(Task(
            name = "start flow",
            flow = flow
        ))

    def get(self):
        return self.queue.get()

    def task_done(self):
        return self.queue.task_done()


api = Queue("api")
default = Queue("default")
bluesky = Queue("bluesky")
mastodon = Queue("mastodon")
reddit = Queue("reddit")
smalltown = Queue("smalltown")