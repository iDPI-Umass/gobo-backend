class RecoverableException(Exception):
  def __init__(self, status):
      self.status = status
      self.recoverable = True

  def __str__(self):
      return self.message