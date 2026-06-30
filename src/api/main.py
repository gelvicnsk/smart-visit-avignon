from fastapi import FastAPI
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(
    title="Smart Visit Avignon API",
    description="API de recommandation touristique pour le Festival d'Avignon",
    version="1.0.0"
)

REQUEST_COUNTER = Counter("smartvisit_requests_total", "Nombre total de requêtes API")


@app.get("/")
def root():
    REQUEST_COUNTER.inc()
    return {
        "message": "Smart Visit Avignon API",
        "status": "running"
    }


@app.get("/health")
def health():
    REQUEST_COUNTER.inc()
    return {
        "status": "ok",
        "service": "smartvisit_api"
    }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
