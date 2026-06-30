"""ETL loading layer — Gold curated data → MongoDB and Neo4j."""

from etl.loading.mongo_loader import MongoDBLoader
from etl.loading.neo4j_loader import Neo4jLoader

__all__ = ["MongoDBLoader", "Neo4jLoader"]
