"""Testes da fabrica: config + data quality (Spark local).

Requer o venv com as dependencias de requirements.txt instaladas.
Roda com: python -m pytest tests -q
"""

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from factory.context import PipelineConfig  # noqa: E402
from factory.quality import run_checks  # noqa: E402
from factory.spark import build_spark  # noqa: E402


def test_load_pipeline_config():
    cfg = PipelineConfig.load(REPO / "config" / "pipelines" / "imdb.yaml")
    assert cfg.name == "imdb"
    assert cfg.abort_on_fail is True
    assert len(cfg.tables) == 4
    assert len(cfg.gold) == 2
    assert cfg.lake_path("bronze", "episode").name == "episode"
    assert cfg.metadata_path().name.startswith("20")


@pytest.fixture(scope="module")
def spark():
    session = build_spark(
        "data-factory-test",
        {"spark.ui.enabled": "false", "spark.driver.memory": "2g"},
        repo_root=REPO,
    )
    yield session
    session.stop()


def test_quality_checks_pass(spark):
    df = spark.createDataFrame(
        [(1, "a", 10), (2, "b", 20)],
        ["id", "name", "value"],
    )
    checks = [
        {"type": "not_empty"},
        {"type": "not_null", "column": "id"},
        {"type": "unique", "columns": ["id"]},
        {"type": "range", "column": "value", "min": 0, "max": 100},
    ]
    results = run_checks(spark, df, "t", "test", checks)
    assert len(results) == 4
    assert all(r.passed for r in results)


def test_quality_checks_fail(spark):
    df = spark.createDataFrame([(1, None), (1, "b")], ["id", "name"])
    checks = [
        {"type": "unique", "columns": ["id"]},
        {"type": "not_null", "column": "name"},
    ]
    results = run_checks(spark, df, "t", "test", checks)
    assert len(results) == 2
    assert all(not r.passed for r in results)
