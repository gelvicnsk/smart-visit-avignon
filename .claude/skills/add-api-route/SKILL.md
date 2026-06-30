---
name: add-api-route
description: Add a new FastAPI route to src/api/main.py with Prometheus counter wiring. Use when adding a new endpoint to the active API.
---

Add a new route to `src/api/main.py` in the smart-visit-avignon project.

$ARGUMENTS should describe the route (e.g. "GET /events returns list of festival events from MongoDB").

1. Read `src/api/main.py` to understand the current structure and existing Prometheus counter patterns.
2. Add the new route following the same conventions:
   - Correct HTTP method and path
   - A `prometheus_client.Counter` for request tracking (match the existing naming pattern)
   - Increment the counter on each call
   - Return a typed JSON response
3. If the route reads from MongoDB or Neo4j, use the env-var connection pattern already present in the file.
4. Do not modify `backend/main.py` — that file is legacy.
