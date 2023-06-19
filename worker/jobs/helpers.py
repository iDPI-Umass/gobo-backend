import models

where = models.helpers.where


def reconcile_sources(person_id, sources):
    desired_sources = []
    for source in sources:
        desired_sources.append(source["id"])
    
    results = models.link.pull({
        "where": [
            where("origin_type", "person"),
            where("origin_id", person_id),
            where("target_type", "source"),
            where("name", "follows")
        ]
    })
   
    current_sources = []
    for result in results:
        current_sources.append(result["target_id"])


    difference = list(set(desired_sources) - set(current_sources)) 
    for source_id in difference:
        models.link.safe_add({
            "origin_type": "person",
            "origin_id": person_id,
            "target_type": "source",
            "target_id": source_id,
            "name": "follows",
            "secondary": None
        })

    difference = list(set(current_sources) - set(desired_sources)) 
    for source_id in difference:
        models.link.find_and_remove({
            "origin_type": "person",
            "origin_id": person_id,
            "target_type": "source",
            "target_id": source_id,
            "name": "follows"
        })