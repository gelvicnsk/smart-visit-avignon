"""Full ETL pipeline orchestrator for Smart Visit Avignon.

Usage
-----
    python jobs/run_pipeline.py                               # all phases
    python jobs/run_pipeline.py --phases ingest transform     # subset
    python jobs/run_pipeline.py --skip-spark                  # skip Spark
    python jobs/run_pipeline.py --dry-run                     # no DB writes

Phases
------
    ingest      Download raw data (Bronze JSON) from APIs / fixture fallback.
    clean       Bronze → Silver Parquet via Apache Spark.
    transform   Silver → Gold: GeoEnricher + GraphBuilder (+ EventsEnricher if Spark).
    load        Gold → MongoDB + Neo4j.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

from etl.config import ETLConfig, etl_config
from etl.utils.logger import configure_logging, get_etl_logger

ALL_PHASES: tuple[str, ...] = ("ingest", "clean", "transform", "load")

# ── Phase functions ────────────────────────────────────────────────────────────


def run_ingest(config: ETLConfig, logger: Any) -> list[Path]:
    """Run all 5 ingesters. Returns the list of Bronze file paths."""
    from etl.ingestion import (
        BikeStationsIngester,
        EventsIngester,
        ParkingsIngester,
        TransportIngester,
        VenuesIngester,
    )

    ingesters = [
        EventsIngester(config=config),
        VenuesIngester(config=config),
        ParkingsIngester(config=config),
        BikeStationsIngester(config=config),
        TransportIngester(config=config),
    ]
    paths: list[Path] = []
    for ingester in ingesters:
        path = ingester.ingest()
        paths.append(path)
        logger.info("ingest.done", source=ingester.source_name, path=str(path))
    return paths


def run_clean(config: ETLConfig, logger: Any) -> list[Path]:
    """Run all 5 Bronze → Silver cleaners (requires Apache Spark)."""
    from etl.cleaning import (
        BikeStationsCleaner,
        EventsCleaner,
        ParkingsCleaner,
        TransportCleaner,
        VenuesCleaner,
    )
    from etl.utils.spark import get_spark, stop_spark

    cleaners = [
        EventsCleaner(config=config),
        VenuesCleaner(config=config),
        ParkingsCleaner(config=config),
        BikeStationsCleaner(config=config),
        TransportCleaner(config=config),
    ]
    spark = get_spark("clean_job")
    paths: list[Path] = []
    try:
        for cleaner in cleaners:
            path = cleaner.run(spark)
            paths.append(path)
            logger.info("clean.done", source=cleaner.source_name, path=str(path))
    finally:
        stop_spark(spark)
    return paths


def run_transform(
    config: ETLConfig,
    logger: Any,
    skip_spark: bool = False,
) -> Path:
    """Build the knowledge graph and (optionally) the enriched events Parquet.

    Always runs GeoEnricher + GraphBuilder on fixture data.
    Runs EventsEnricher (Spark) only when ``skip_spark=False`` and the Silver
    events/venues Parquet files exist.

    Returns:
        Path to the curated graph directory.
    """
    from etl.fixtures.events_fixture import get_events
    from etl.fixtures.mobility_fixture import get_bike_stations, get_bus_stops, get_parkings
    from etl.fixtures.venues_fixture import get_venues
    from etl.transformation import GraphBuilder

    events = get_events()
    venues = get_venues()
    parkings = get_parkings()
    bike_stations = get_bike_stations()
    bus_stops = get_bus_stops()

    gb = GraphBuilder(config=config)
    graph_path = gb.run(events, venues, parkings, bike_stations, bus_stops)
    logger.info("transform.graph_done", path=str(graph_path))

    if not skip_spark:
        events_silver = config.processed_data_path / "events"
        venues_silver = config.processed_data_path / "venues"
        if events_silver.exists() and venues_silver.exists():
            from etl.transformation import EventsEnricher
            from etl.utils.spark import get_spark, stop_spark

            spark = get_spark("transform_job")
            try:
                out = EventsEnricher(config=config).run(spark)
                logger.info("transform.enrichment_done", path=str(out))
            finally:
                stop_spark(spark)
        else:
            logger.warning(
                "transform.enrichment_skipped",
                reason="Silver Parquet not found — run the clean phase first",
            )

    return graph_path


def run_load(
    config: ETLConfig,
    graph_path: Path,
    logger: Any,
    dry_run: bool = False,
) -> None:
    """Load curated data into MongoDB and Neo4j.

    Uses fixture data for MongoDB (always available) and graph JSON files
    for Neo4j (output of the transform phase).
    """
    from etl.fixtures.events_fixture import get_events
    from etl.fixtures.mobility_fixture import get_bike_stations, get_bus_stops, get_parkings
    from etl.fixtures.venues_fixture import get_venues
    from etl.loading import MongoDBLoader, Neo4jLoader

    events = get_events()
    venues = get_venues()
    parkings = get_parkings()
    bike_stations = get_bike_stations()
    bus_stops = get_bus_stops()

    if dry_run:
        logger.info("load.dry_run_skipped")
        return

    with MongoDBLoader(config=config) as mongo:
        total = mongo.run(events, venues, parkings, bike_stations, bus_stops)
        logger.info("load.mongo_done", records=total)

    with Neo4jLoader(config=config) as neo4j:
        nodes, edges = neo4j.run(graph_path)
        logger.info("load.neo4j_done", nodes=nodes, edges=edges)


# ── CLI entry point ────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None, config: ETLConfig | None = None) -> int:
    """Run the pipeline. Returns exit code (0 = success, 1 = error)."""
    parser = argparse.ArgumentParser(
        prog="run_pipeline",
        description="Smart Visit Avignon — ETL Pipeline",
    )
    parser.add_argument(
        "--phases",
        nargs="+",
        choices=list(ALL_PHASES),
        default=list(ALL_PHASES),
        metavar="PHASE",
        help=f"Phases to run (default: all). Choices: {', '.join(ALL_PHASES)}",
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip Spark-dependent phases (clean, events enrichment)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full pipeline but skip database writes",
    )
    args = parser.parse_args(argv)
    phases = set(args.phases)

    configure_logging()
    logger = get_etl_logger("run_pipeline")
    cfg = config or etl_config
    cfg.ensure_directories()

    logger.info(
        "pipeline.start",
        phases=sorted(phases),
        skip_spark=args.skip_spark,
        dry_run=args.dry_run,
    )
    t0 = time.monotonic()
    graph_path = cfg.graph_data_path

    try:
        if "ingest" in phases:
            t = time.monotonic()
            run_ingest(cfg, logger)
            logger.info("pipeline.phase_done", phase="ingest",
                        elapsed_s=round(time.monotonic() - t, 1))

        if "clean" in phases:
            if args.skip_spark:
                logger.info("pipeline.phase_skipped", phase="clean", reason="--skip-spark")
            else:
                t = time.monotonic()
                run_clean(cfg, logger)
                logger.info("pipeline.phase_done", phase="clean",
                            elapsed_s=round(time.monotonic() - t, 1))

        if "transform" in phases:
            t = time.monotonic()
            graph_path = run_transform(cfg, logger, skip_spark=args.skip_spark)
            logger.info("pipeline.phase_done", phase="transform",
                        elapsed_s=round(time.monotonic() - t, 1))

        if "load" in phases:
            t = time.monotonic()
            run_load(cfg, graph_path, logger, dry_run=args.dry_run)
            logger.info("pipeline.phase_done", phase="load",
                        elapsed_s=round(time.monotonic() - t, 1))

    except Exception as exc:
        logger.error("pipeline.failed", error=str(exc))
        return 1

    logger.info("pipeline.complete", total_elapsed_s=round(time.monotonic() - t0, 1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
