# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Project

All services run via Docker Compose:

```bash
docker-compose up -d           # Start all 9 services
docker-compose down            # Stop services
docker-compose logs -f api     # Follow API logs
```

Local dev without Docker:

```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
streamlit run src/dashboard/app.py --server.address=0.0.0.0 --server.port=8501
pip install -r requirements.txt
```

## Service Map

| Service | Host port | Notes |
|---|---|---|
| `api` | 8005 | `src/api/main.py` — the active FastAPI app |
| `dashboard` | 8501 | Streamlit; connects to `api` via internal Docker DNS |
| `spark-master` | 8080, 7077 | |
| `spark-worker` | 8081 | |
| `mongodb` | 27017 | DB name: `smartvisit` |
| `neo4j` | 7474, 7687 | |
| `redis` | 6379 | |
| `prometheus` | 9095 | Scrapes `api:8000` every 15 s |
| `grafana` | 3000 | |

## Environment Variables

Copy `.env.example` to `.env`. Key variables and their live values:

```
MONGO_URI=mongodb://mongodb:27017
MONGO_DB=smartvisit           # .env.example says festival_avignon — outdated
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password123    # must match NEO4J_AUTH in docker-compose.yml
REDIS_HOST=redis
REDIS_PORT=6379
```

## Gotchas

- **Two FastAPI apps exist.** `backend/main.py` is legacy and not referenced in `docker-compose.yml`. The active app is `src/api/main.py`. Do not add features to `backend/`.
- **Dashboard uses internal Docker DNS** (`http://api:8000/health`). Running it outside Docker requires overriding that hostname.
- **`src/api/__pycache__/` is owned by root** from previous Docker runs with `./src` bind-mounted. If you hit `PermissionError` locally, fix with: `sudo chown -R $USER src/`
- **No git repository yet.** Run `git init` before first commit. `.env` exists on disk and will be staged unless `.gitignore` is respected — double-check with `git status` before committing.
- **`Projet_2026.pdf:Zone.Identifier`** is a Windows ADS stub file. It can be safely deleted.

## Python Versions

- Local `.venv`: 3.12
- `docker/api/` and `docker/dashboard/` containers: 3.11
- `backend/` (legacy): 3.10

Write code compatible with Python 3.11 for anything that runs inside containers.

## Project Scaffold Status

Most directories are empty stubs waiting to be built:
`etl/`, `jobs/`, `notebooks/`, `docs/`, `data/raw`, `data/processed`, `data/curated`,
`src/ingestion/`, `src/processing/`, `src/recommendation/`, `src/storage/`
