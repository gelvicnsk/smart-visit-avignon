from fastapi import FastAPI
import os
from pymongo import MongoClient
from neo4j import GraphDatabase

app = FastAPI(title="Smart-Visit Festival Avignon")

# Connexions aux bases (initialisées au démarrage)
mongo_client = None
neo4j_driver = None

@app.on_event("startup")
async def startup():
    global mongo_client, neo4j_driver
    # MongoDB
    mongo_uri = os.getenv("MONGO_URI", "mongodb://root:rootpass@mongodb:27017")
    mongo_client = MongoClient(mongo_uri)
    # Neo4j
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_pass = os.getenv("NEO4J_PASSWORD", "test1234")
    neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass))

@app.on_event("shutdown")
async def shutdown():
    if mongo_client:
        mongo_client.close()
    if neo4j_driver:
        neo4j_driver.close()

@app.get("/")
async def root():
    return {"message": "Smart-Visit API is running"}

@app.get("/health")
async def health():
    return {"status": "ok"}
