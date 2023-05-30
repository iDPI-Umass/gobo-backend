import logging
import http_errors

def parse_query(views, data):
    view = data.get("view") or "created"
    direction = data.get("direction") or "descending"

    try:
        per_page = int(data.get("per_page") or 25)
    except Exception as e:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")

    try:
        page = int(data.get("page") or 1)
    except Exception as e:
        raise http_errors.bad_request(f"page {page} is invalid")


    if view not in views:
        raise http_errors.bad_request(f"view {view} is invalid")
    if direction not in ["ascending", "descending"]:
        raise http_errors.bad_request(f"direction {direction} is invalid")
    if per_page < 1:
        raise http_errors.bad_request(f"per_page {per_page} is invalid")
    if page < 1:
        raise http_errors.bad_request(f"page {page} is invalid")

    return {
      "view": view,
      "direction": direction,
      "per_page": per_page,
      "page": page
    }