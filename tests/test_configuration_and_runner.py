"""Fast contract and orchestration tests; no Spark session or real table writes."""

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import run_week1
from scripts.render_benchmark_report import render
from src.common.config import build_ingestors, load_config, validate_config
from src.integration.pipeline import UrbanDataIntegrationPipeline


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "config/platform_config.yaml")

    def test_invalid_config_fails_before_spark(self):
        changes = [
            lambda c: c["datasets"].pop("weather"),
            lambda c: c["datasets"].update(typo={}),
            lambda c: c["datasets"]["weather"].pop("source_path"),
            lambda c: c["datasets"]["weather"]["validation"].update(
                broken="temp > {missing}"
            ),
            lambda c: c["datasets"]["air_quality"]["casts"].update(MDL="double"),
            lambda c: c["spark"].update(shuffle_partitions=0),
            lambda c: c["benchmark"].update(iterations=-1),
            lambda c: c["integration"].update(
                output_path=c["datasets"]["weather"]["silver_path"]
            ),
        ]
        for change in changes:
            config = copy.deepcopy(self.config)
            change(config)
            with (
                self.subTest(config=config),
                patch.object(run_week1, "get_spark_session") as start,
            ):
                with self.assertRaises(ValueError):
                    run_week1.run(config)
                start.assert_not_called()
        for config in (None, [], "invalid"):
            with self.assertRaises(ValueError):
                validate_config(config)

    def test_empty_integration_partitions_and_constructor_ownership(self):
        self.config["integration"]["partition_cols"] = []
        pipeline = UrbanDataIntegrationPipeline.from_config(None, self.config)
        self.assertEqual(pipeline.partition_cols, [])
        ingestors = build_ingestors(None, self.config)
        ingestors["taxi_trips"].quality_rules["max_fare_amount"] = 10
        self.assertEqual(
            self.config["datasets"]["taxi_trips"]["quality_rules"]["max_fare_amount"],
            5000,
        )
        with self.assertRaises(TypeError):
            type(ingestors["taxi_trips"])(
                None, dataset_spec=self.config["datasets"]["taxi_trips"]
            )

    def test_summary_replacement_preserves_previous_latest_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "metrics.json"
            output.write_text('{"previous": true}')
            with patch.object(Path, "replace", side_effect=OSError("replace failed")):
                with self.assertRaises(OSError):
                    run_week1.save_summary_metrics({"new": True}, output)
            self.assertEqual(json.loads(output.read_text()), {"previous": True})
            self.assertFalse(list(Path(directory).glob("*.tmp")))
            artifact = run_week1.save_summary_metrics({"new": True}, output)
            self.assertEqual(artifact.read_text(), output.read_text())

    def test_report_requires_current_input_and_stable_layout_order(self):
        names = [
            "Strategy C (Monthly Partitioned)",
            "Strategy A (Unpartitioned)",
            "Strategy B (Date Partitioned)",
        ]
        storage = {
            name: {
                "ingestion_time_sec": n,
                "storage_size_mb": n,
                "num_parquet_files": n,
                "storage_size_bytes": n,
            }
            for n, name in enumerate(names, 1)
        }
        benchmark = {
            "run_id": "fixture",
            "total_records": 1,
            "input_kind": "raw_taxi_parquet",
            "results_equivalent": True,
            "storage_metrics": storage,
            "query_statistics": {
                name: {"Q1": {"mean_ms": 1, "stdev_ms": 0}} for name in names
            },
            "methodology": "Fixture measurements.",
        }
        metrics = {
            "status": "success",
            "schema_version": "2.0.0",
            "runtime": {"python": "fixture", "spark": "fixture", "delta": "fixture"},
            "benchmark": benchmark,
        }
        with tempfile.TemporaryDirectory() as directory:
            source, output = (
                Path(directory) / "metrics.json",
                Path(directory) / "report.md",
            )
            source.write_text(json.dumps(metrics))
            render(source, output)
            self.assertIn("| Taxi ingestion time (s) | 2 | 3 | 1 |", output.read_text())
            for kind in (None, "integrated_taxi"):
                benchmark["input_kind"] = kind
                source.write_text(json.dumps(metrics))
                with self.assertRaisesRegex(ValueError, "input_kind"):
                    render(source, output)


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "config/platform_config.yaml")
        self.spark = MagicMock(version="fixture")
        self.ingestors = {name: MagicMock() for name in self.config["datasets"]}
        self.pipeline = MagicMock()
        self.pipeline.partition_cols = []
        self.pipeline.run.return_value = {"total_records": 1}
        self.patches = [
            patch.object(run_week1, "get_spark_session", return_value=self.spark),
            patch.object(run_week1, "stop_spark_session"),
            patch.object(run_week1, "build_ingestors", return_value=self.ingestors),
            patch.object(
                run_week1.UrbanDataIntegrationPipeline,
                "from_config",
                return_value=self.pipeline,
            ),
            patch.object(
                run_week1, "save_summary_metrics", return_value=Path("fixture.json")
            ),
            patch.object(run_week1, "render"),
            patch.object(run_week1, "StorageBenchmarkRunner"),
        ]
        self.mocks = [p.start() for p in self.patches]
        for p in self.patches:
            self.addCleanup(p.stop)

    def test_preflight_failure_prevents_all_writes(self):
        self.ingestors["weather"].preflight.side_effect = ValueError("bad input schema")
        with self.assertRaisesRegex(ValueError, "bad input schema"):
            run_week1.run(self.config)
        for ingestor in self.ingestors.values():
            ingestor.run.assert_not_called()
        self.pipeline.run.assert_not_called()
        self.mocks[1].assert_called_once_with(self.spark)

    def test_artifact_failure_is_fatal_and_spark_stops(self):
        for mock in (self.mocks[4], self.mocks[5]):
            with self.subTest(artifact=mock):
                mock.side_effect = OSError("artifact failed")
                with self.assertRaisesRegex(OSError, "artifact failed"):
                    run_week1.run(self.config)
                mock.side_effect = None
        self.assertEqual(self.mocks[1].call_count, 2)

    def test_original_failure_survives_secondary_summary_failure(self):
        self.pipeline.run.side_effect = ValueError("original failure")
        self.mocks[4].side_effect = OSError("summary failed")
        with self.assertRaisesRegex(ValueError, "original failure"):
            run_week1.run(self.config, skip_benchmark=True)
        self.mocks[1].assert_called_once_with(self.spark)

    def test_pipeline_run_id_is_shared(self):
        result = run_week1.run(self.config, skip_benchmark=True)
        for ingestor in self.ingestors.values():
            ingestor.run.assert_called_once_with(run_id=result["run_id"])
        self.assertEqual(result["status"], "success")
        self.mocks[6].assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
