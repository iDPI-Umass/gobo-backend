import logging
import os
import mimetypes
from flask import request
import http_errors
import models
import joy

def check_proof_data(person_id, data):
    if data["person_id"] != person_id:
        raise http_errors.bad_request("proof person_id does not match resource")



def person_proofs_post(person_id): 
    data = dict(request.json)
    check_proof_data(person_id, data)
    proof = models.proof.add(data)
    return {"content": proof}


def person_proof_put(person_id, id):
    proof = models.proof.find({
        "person_id": person_id,
        "id": id
    })
    if proof is None:
        raise http_errors.not_found(f"proof {person_id} / {id} is not found")
    
    data = dict(request.json)
    check_proof_data(person_id, data)
    proof = models.proof.update(id, data) 
    return {"content": proof}

def person_proof_delete(person_id, id):
    proof = models.proof.find({
        "person_id": person_id,
        "id": id
    })

    if proof is None:
        raise http_errors.not_found(
            f"proof {person_id} / {id} is not found"
        )

    models.proof.remove(id)

    # 204 Response
    return {"content": ""}