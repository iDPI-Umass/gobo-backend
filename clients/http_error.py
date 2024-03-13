class HTTPError(Exception):
  def __init__(self, status, body, url = ""):
      self.status = status
      if body is None:
          body = {}
      self.body = body
      self.url = url

  def __str__(self):
      return f"Got response status {self.status} for {self.url}"