import logging
import time
import models
import queues


def dispatch(task):
    if task.name != "poll":
        logging.info("dispatching: %s", task)

    if task.name == "poll":
        return poll_database(task)
    else:
        logging.warning("No matching job for task: %s", task)


def poll_database(task):
    task.quiet()

    query = {
        "per_page": 50,
        "page": 1,
        "view": "created",
        "direction": "ascending",
        "where": []
    }

    last = task.details.get("last")
    if last != None:
        query["where"].append({
            "key": "created",
            "value": last["created"],
            "operator": "gte"
        })
        query["where"].append({
            "key": "id",
            "value": last["id"],
            "operator": "neq"
        })

    tasks = models.task.query(query)

    for _task in tasks:
        queues.shard_task_dict(_task)

    if len(tasks) == 0:
        time.sleep(1)
    else:
        task.update({"last": tasks[-1]})

    new_task = queues.Task.from_details("poll", task.details)
    new_task.quiet()
    queues.api.put_task(new_task)