from flask import jsonify

# Authorization Error, Adapted from https://auth0.com/docs/quickstart/backend/python/01-authorization
class AuthError(Exception):
  def __init__(self, error, status_code):
    self.error = error
    self.status_code = status_code

def handle_auth_error(ex):
  response = jsonify(ex.error)
  response.status_code = ex.status_code
  return response

class ArgError(Exception):
  def __init__(self, error, status_code):
    self.error = error
    self.status_code = status_code

def handle_arg_error(ex):
  response = jsonify(ex.error)
  response.status_code = ex.status_code
  return response
