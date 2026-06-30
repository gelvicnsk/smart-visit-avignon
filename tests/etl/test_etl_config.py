"""Unit tests for etl/config.py — no network, no Spark, no database."""

from pathlib import Path

from etl.config import PROJECT_ROOT, ETLConfig, etl_config


class TestProjectRoot:
    def test_is_a_directory(self):
        assert PROJECT_ROOT.is_dir()

    def test_contains_src_package(self):
        assert (PROJECT_ROOT / "src").is_dir()

    def test_contains_etl_package(self):
        assert (PROJECT_ROOT / "etl").is_dir()


class TestETLConfigDefaults:
    def test_singleton_type(self):
        assert isinstance(etl_config, ETLConfig)

    def test_raw_data_path_name(self):
        assert etl_config.raw_data_path.name == "raw"

    def test_processed_data_path_name(self):
        assert etl_config.processed_data_path.name == "processed"

    def test_curated_data_path_name(self):
        assert etl_config.curated_data_path.name == "curated"

    def test_graph_data_path_is_under_curated(self):
        assert etl_config.graph_data_path.parent == etl_config.curated_data_path

    def test_default_spark_mode_is_local(self):
        cfg = ETLConfig()
        assert cfg.spark_master_url == "local[*]"

    def test_spark_cluster_url(self):
        cfg = ETLConfig(spark_master_host="spark-master")
        assert cfg.spark_master_url == "spark://spark-master:7077"

    def test_spark_cluster_url_custom_port(self):
        cfg = ETLConfig(spark_master_host="spark-master", spark_master_port=7099)
        assert cfg.spark_master_url == "spark://spark-master:7099"

    def test_use_fixtures_fallback_is_true_by_default(self):
        cfg = ETLConfig()
        assert cfg.use_fixtures_fallback is True

    def test_proximity_threshold_is_positive(self):
        assert etl_config.proximity_threshold_m > 0

    def test_walking_speed_is_positive(self):
        assert etl_config.walking_speed_m_per_min > 0


class TestEnsureDirectories:
    def test_creates_all_four_directories(self, tmp_path: Path):
        cfg = ETLConfig(
            raw_data_path=tmp_path / "raw",
            processed_data_path=tmp_path / "processed",
            curated_data_path=tmp_path / "curated",
        )
        cfg.ensure_directories()

        assert (tmp_path / "raw").is_dir()
        assert (tmp_path / "processed").is_dir()
        assert (tmp_path / "curated").is_dir()
        assert (tmp_path / "curated" / "graph").is_dir()

    def test_idempotent_when_directories_already_exist(self, tmp_path: Path):
        cfg = ETLConfig(
            raw_data_path=tmp_path / "raw",
            processed_data_path=tmp_path / "processed",
            curated_data_path=tmp_path / "curated",
        )
        cfg.ensure_directories()
        # Second call must not raise
        cfg.ensure_directories()
