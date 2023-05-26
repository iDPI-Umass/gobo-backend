from flask import Flask
from yaml import safe_load

app = Flask(__name__)

@app.route("/")
def discovery():
    with open("api.yaml", "r") as file:
        return safe_load(file.read())

@app.route("/people", methods = ["post"])
def create_person():
    return { "foo": "foo" }, 201

app.run(host="0.0.0.0", debug=True)