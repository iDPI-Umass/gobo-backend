class HTTPError(Exception):
  def __init__(self, status, body):
      self.status = status
      if body is None:
          body = {}
      self.body = body

  def __str__(self):
      return f"Got response status {self.status}"