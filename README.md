# TrafficGym

A Django + Celery platform for defining, running, and analysing reproducible SUMO traffic simulation experiments.

Experiments are Python classes (or visual pipelines built in the graph builder) that subclass `Experiment` and implement a `run()` method. The platform handles scheduling, seeding, real-time progress, subscription logging, and analytics.

## Stack

| Component | Role |
|---|---|
| Django 6 | Web UI and REST endpoints |
| Celery 5 | Distributed task queue — runs simulations in parallel |
| Redis | Celery broker |
| PostgreSQL 16 | Persistent store for scenarios, experiments, runs, artefacts |
| libsumo 1.25 | In-process SUMO bindings (no subprocess overhead) |

## Quick start (Docker)

**Requirements:** Docker with the Compose plugin (`docker compose version`).

```bash
git clone https://github.com/AlessioRimoldi/TrafficGym.git
cd TrafficGym

cp .env.example .env          # review and edit if needed
docker compose up --build
```

The first build takes a few minutes (downloads ~700 MB including SUMO). Once running:

| URL | Service |
|---|---|
| http://localhost:8000 | Web UI |
| http://localhost:8000/docs/ | Documentation |

Database migrations run automatically on startup.

**Create an admin user (once):**

```bash
docker compose exec web python src/trafficgym/interface/manage.py createsuperuser
```

**Day-to-day:**

```bash
docker compose up          # start
docker compose down        # stop
docker compose up --build  # rebuild after code changes
```

**sumo-gui (Open GUI mode):**

sumo-gui opens on the machine running the Celery worker, not in the browser. On Linux, allow Docker to connect to your display before starting:

```bash
xhost +local:docker
```

Then set `DISPLAY` in `.env` to your session value (usually `:0`). See `.env.example` for macOS and Windows instructions.

## Development setup (without Docker)

**Requirements:** Python ≥ 3.10, Node.js, PostgreSQL, Redis, SUMO 1.25.

```bash
# Python
python -m venv venv && source venv/bin/activate
pip install -e .
pip install eclipse-sumo==1.25.0

# TypeScript
cd src/trafficgym/interface
npm install && npm run build
cd ../../..

# Database
createdb trafficgym

# Start services (in separate terminals)
redis-server
celery -A trafficgym.interface.core worker -l INFO
python src/trafficgym/interface/manage.py migrate
python src/trafficgym/interface/manage.py runserver
```

Set `DB_USER` to your local PostgreSQL user in `src/trafficgym/interface/config/settings.py`, or override via environment variables (see settings.py for the full list).

## Project layout

```
src/trafficgym/
  engine/
    adapters/        libsumo adapter (SimulationPort implementation)
    control/         controller blocks, aggregators, codegen, block registry
    experiment.py    Experiment base class
    ports/           SimulationPort abstract interface
    transformations/ netconvert, inspect, netpreview handlers
  interface/
    core/            Django app — models, views, Celery tasks, URL routing
    config/          Django settings, WSGI, ASGI
    manage.py
sumo_files/          Reference SUMO scenarios (ramp_meter, service_station, …)
docs/                Sphinx source (RST) — served at /docs/ when the app runs
examples/            Standalone experiment scripts
```

## Writing experiments

See the full workflow guide and API reference at `/docs/` once the app is running, or read `docs/getting_started.rst` directly.
