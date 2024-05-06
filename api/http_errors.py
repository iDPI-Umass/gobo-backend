

class HTTPError(Exception):
  def __init__(self, status, message):
      self.status = status
      self.message = message

  def __str__(self):
      return f"status {self.status}: {self.message}"


def bad_request(message):
    return HTTPError(400, message)
def unauthorized(message):
    return HTTPError(401, message)
def forbidden(message):
    return HTTPError(403, message)
def not_found(message):
    return HTTPError(404, message)
def conflict(message):
    return HTTPError(409, message)
def content_too_large(message):
    return HTTPError(413, message)
def unprocessable_content(message):
    return HTTPError(422, message)
