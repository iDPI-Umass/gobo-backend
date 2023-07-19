import logging
import re
from flask import g
import http_errors
import models

def parse_page_query(data):
    try:
        per_page = int(data.get("per_page") or 25)
    except Exception as e:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")

    try:
        page = int(data.get("page") or 1)
    except Exception as e:
        raise http_errors.bad_request(f"page {page} is invalid")

    if per_page < 1:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")
    if per_page > 100:
        per_page = 100
    if page < 1:
        raise http_errors.bad_request(f"page {page} is invalid")

    return {
      "per_page": per_page,
      "page": page
    }


def parse_query(views, data):
    pages = parse_page_query(data)
    view = data.get("view") or "created"
    direction = data.get("direction") or "descending"
    
    if view not in views:
        raise http_errors.bad_request(f"view {view} is invalid")
    if direction not in ["ascending", "descending"]:
        raise http_errors.bad_request(f"direction {direction} is invalid")


    return {
      "view": view,
      "direction": direction,
      "per_page": pages["per_page"],
      "page": pages["page"],
      "where": []
    }


def parse_base_url(data):
    url = data["base_url"].lower()
    is_domain = re.match("^https:\/\/([a-z0-9]+[.])+([a-z0-9])+$", url)
    is_ipv4 = re.match("^https:\/\/(?:[0-9]{1,3}\.){3}[0-9]{1-3}$", url)

    if not is_domain and not is_ipv4:
        raise http_errors.bad_request(
            "base_url must either be an ipv4 address or a domain name that " +
            "does not end with a forward slash (e.g. https://gobo.social " +
            "or https://www.reddit.com)"
        )
    
    return url

def get_viewer(id):
    try:
        person = g.person
    except Exception as e:
        person = models.person.get(id)
    
    return person