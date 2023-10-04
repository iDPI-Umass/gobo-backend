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
        queue = _task["queue"]

        if queue == "default":
            queues.default.put_dict(_task)
        elif queue == "bluesky":
            queues.bluesky.put_dict(_task)
        elif queue == "reddit":
            queues.reddit.put_dict(_task)
        elif queue == "mastodon":
            queues.mastodon.put_dict(_task)
        elif queue == "smalltown":
            queues.smalltown.put_dict(_task)
        else:
            logging.warning("No matching queue for task: %s", _task)


    if len(tasks) == 0:
        time.sleep(10)
    else:
        task.update({"last": tasks[-1]})

    new_task = queues.Task(
        name = "poll",
        details = task.details
    )
    
    new_task.quiet()
    queues.api.put_task(new_task)