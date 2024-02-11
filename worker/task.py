import logging
import joy
import models


class Task():
    def __init__(self, name, priority = 10, details = None, id = None, tries = 0, flow = [], failure = None):
        self.id = id or joy.crypto.random({"encoding": "safe-base64"})
        self.name = name
        self.reset_tracking()
        self.priority = priority
        self.details = details or {}
        self.flow = flow
        self.fail_function = failure

        self.is_quiet = False
        self.is_halted = False

    def __repr__(self): 
        details = {}
        for key, value in self.details.items():
            details[key] = value

        return str({
          "id": self.id,
          "name": self.name,
          "priority": self.priority,
          "tries": self.tries,
          "created": self.created,
          "updated": self.updated,
        })
    
    def __str__(self): 
        return self.__repr__()
    
    def __lt__(self, task):
        return self.priority < task.priority
    

    @staticmethod
    def from_dict(task):
        return Task(
          id = task["id"],
          name = task["name"],
          priority = task.get("priority", 10),
          details = task["details"]
        )
    
    @staticmethod
    def from_details(name, details = {}, priority = None):
        return Task(
            name = name,
            details = details,
            priority = priority
        )


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

    def failure(self):
        if self.fail_function is not None:
            self.fail_function(self)

    def progress(self, queues, result = {}):
        result = result or {}
        if self.is_halted == True:
            return

        next = None
        if len(self.flow) > 0:
            next = self.flow.pop(0)
        
        if next is not None:
            self.name = next["name"]
            self.priority = next.get("priority", self.priority)
            self.reset_tracking()
            self.details.update(result)
            self.details.update(next.get("details", {}))
            queues.shard_task(next["queue"], self)