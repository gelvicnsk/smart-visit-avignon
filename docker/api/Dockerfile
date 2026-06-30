FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    pymongo \
    neo4j \
    redis \
    pandas \
    requests \
    prometheus-client

COPY . .

EXPOSE 8000
