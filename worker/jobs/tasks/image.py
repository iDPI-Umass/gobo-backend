import logging
import os
import joy
import models
import queues

where = models.helpers.where
QueryIterator = models.helpers.QueryIterator

def prune_image_cache(task):
    time_limit = joy.time.to_iso_string(joy.time.hours_ago(12))

    drafts = QueryIterator(
        model = models.draft_image,
        wheres = [
            where("created", time_limit , "lt")
        ]
    )

    for draft in drafts:
        filename = os.path.join(
            os.environ.get("UPLOAD_DIRECTORY"), 
            draft["id"]
        )
        
        if os.path.exists(filename):
            os.remove(filename)
        
        models.draft_image.remove(draft["id"])