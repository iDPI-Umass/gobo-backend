import logging
import queue
from task import Task


class Queue():
    def __init__(self, name):
        self.name = name
        self.queue = queue.PriorityQueue()

    def put_task(self, task):
        self.queue.put(task)

    def put_details(self, name, details = None, priority = 10):
        task = Task.from_details(name, details, priority)
        self.put_task(task)

    def put_dict(self, task):
        task = Task.from_dict(task)
        self.put_task(task)

    def put_flow(self, priority, flow, failure = None):
        task = Task(
            name = "start flow",
            priority = priority,
            flow = flow,
            failure = failure
        )

        self.put_task(task)

    def get(self):
        return self.queue.get()

    def task_done(self):
        return self.queue.task_done()


api = Queue("api")
default = Queue("default")
# Platform queues are sharded to limit per-account concurrency.
shard_counts = {}
bluesky = []
linkedin = []
mastodon = []
reddit = []
smalltown = []

def build_sharded_queues(counts):
    shard_counts.update(counts)
    for i in range(counts["bluesky"]):
        bluesky.append(Queue(f"bluesky {i}"))
    for i in range(counts["linkedin"]):
        linkedin.append(Queue(f"linkedin {i}"))
    for i in range(counts["mastodon"]):
        mastodon.append(Queue(f"mastodon {i}"))
    for i in range(counts["reddit"]):
        reddit.append(Queue(f"reddit {i}"))
    for i in range(counts["smalltown"]):
        smalltown.append(Queue(f"smalltown {i}"))




import hashlib

# Based on uniform hashing algorithm here:
# https://www.d.umn.edu/~gshute/cs2511/slides/hash_tables/sections/uniform_hashing.xhtml
def uniform_shard(string, count):    
    m = hashlib.sha512()
    m.update(bytearray(string, "utf-8"))
    byte_array = m.digest()
    
    result = 1
    for value in byte_array:
      result = (result * 31) + value
    return result % count

def get_shard(platform, task):
    identity = task.details.get("identity")
    if identity is None:
        raise Exception("cannot shard a task that lacks an identity")
    
    base_url = identity.get("base_url")
    if base_url is None:
        raise Exception("cannot shard a task with identity that lacks base_url")
    
    platform_id = identity.get("platform_id")
    if platform_id is None:
        raise Exception("cannot shard a task with identity that lacks platform_id")
    
    string = base_url + platform_id
    count = shard_counts[platform]
    return uniform_shard(string, count)


def shard_task(platform, task):
  if platform == "default":
      return default.put_task(task)
  if platform == "api":
      return api.put_task(task)

  shard = get_shard(platform, task)
  if platform == "bluesky":
      return bluesky[shard].put_task(task)
  if platform == "linkedin":
      return linkedin[shard].put_task(task)
  if platform == "mastodon":
      return mastodon[shard].put_task(task)
  if platform == "reddit":
      return reddit[shard].put_task(task)
  if platform == "smalltown":
      return smalltown[shard].put_task(task)

  logging.warning({
      "platform": platform, 
      "task": task
  })
  raise Exception("unable to shard task")


def shard_task_dict(task):
    platform = task.get("queue", "default")
    task = Task.from_dict(task)
    return shard_task(platform, task)

def shard_task_details(platform, name, details = {}, priority = None):
    task = Task.from_details(name, details, priority)
    return shard_task(platform, task)