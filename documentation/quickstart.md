# Quickstart

## Requirements
- git
- Docker and Docker Compose
- `.env` secrets that are not committed as part of this repository

## Instructions

Pull this repo and use the `docker` branch.

```bash
git clone git@github.com:iDPI-Umass/gobo-backend.git
cd gobo-backend
git checkout docker
```


Add the `.env` files to the `/api/.env` and `/worker/.env` paths.

Start the Docker engine/daemon on your machine so you can use Docker commands.

Build this project.

```bash
docker compose -f docker-compose-development.yaml build
```

That will handle pulling down the containers and building up the internal code based on any dependencies. You'll get two Python images (one running Flask) and a PostgreSQL database image.

Once those are done building, you can start them with:
```bash
docker compose -f docker-compose-development.yaml up
```


Initialize the database schema by opening a _new_ terminal while the containers are running:
```
docker compose -f docker-compose-development.yaml exec api bash
```

This will shell you into the API container's process. Run the initialization script.
```bash
python setup_db.py
```


At this point, your installation is complete, but your example system will be empty.

