---
name: etl-scaffold
description: Scaffold a new PySpark ETL job in etl/ configured for this project's Spark/MongoDB/Neo4j setup. Use when creating a new data ingestion, transformation, or processing script.
disable-model-invocation: true
---

Create a new PySpark ETL job for the smart-visit-avignon project.

$ARGUMENTS should contain the job name (e.g. "ingest-events" or "process-venues").

1. Create `etl/<job-name>.py` with:
   - `SparkSession` connecting to `spark://spark-master:7077`; fall back to `local[*]` when `SPARK_MASTER_HOST` is unset
   - Env vars loaded via `python-dotenv` (`MONGO_URI`, `MONGO_DB`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`)
   - A clear `extract()`, `transform()`, `load()` structure inside `main()`
   - MongoDB I/O using `spark.read.format("mongodb")` / `write.format("mongodb")`
   - `if __name__ == "__main__": main()` entry point

2. After creating the file, show how to submit it:
   ```bash
   docker-compose exec spark-master spark-submit /app/etl/<job-name>.py
   ```
