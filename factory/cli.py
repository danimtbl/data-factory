"""CLI da Data Factory.

Uso:
    python -m factory list
    python -m factory run imdb [--layers bronze,silver,gold] [--source-base <path>]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from factory.aggregate import build_gold
from factory.context import PipelineConfig
from factory.ingest import ingest_bronze
from factory.metadata import RunMetadata
from factory.quality import run_checks
from factory.spark import build_spark
from factory.storage import get_storage
from factory.transform import transform_silver


def _repo_root(arg_repo: str | None) -> Path:
    return Path(arg_repo) if arg_repo else Path(__file__).resolve().parents[1]


def _pipeline_yaml(repo: Path, name: str) -> Path:
    path = repo / "config" / "pipelines" / f"{name}.yaml"
    if not path.exists():
        sys.exit(f"pipeline {name!r} nao encontrado em {path}")
    return path


def _quality_gate(spark: Any, cfg: PipelineConfig, meta: RunMetadata, layer: str, table: str) -> None:
    """Roda os checks de uma tabela/camada e aborta se houver FAIL (quando configurado)."""
    checks = [
        entry["checks"]
        for entry in cfg.checks
        if entry.get("layer") == layer and entry.get("table") == table
    ]
    if not checks:
        return
    checks = checks[0]
    storage = get_storage()
    df = spark.read.format("delta").load(storage.delta_uri(cfg.lake_path(layer, table)))
    results = run_checks(spark, df, table, layer, checks)
    meta.add_checks(layer, table, results)
    failed = [r for r in results if not r.passed]
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [dq {layer}.{table}] {r.check.get('type')}: {status} {r.detail}")
    if failed and cfg.abort_on_fail:
        sys.exit(
            f"quality gate falhou em {layer}.{table}: "
            f"{[r.check.get('type') for r in failed]}"
        )


def cmd_list(args: argparse.Namespace) -> None:
    repo = _repo_root(args.repo)
    pipelines_dir = repo / "config" / "pipelines"
    names = sorted(p.stem for p in pipelines_dir.glob("*.yaml"))
    if not names:
        print(f"nenhum pipeline em {pipelines_dir}")
        return
    print("pipelines disponiveis:")
    for name in names:
        print(f"  - {name}")


def cmd_run(args: argparse.Namespace) -> None:
    repo = _repo_root(args.repo)
    yaml_path = _pipeline_yaml(repo, args.pipeline)
    cfg = PipelineConfig.load(yaml_path, source_base=args.source_base)
    meta = RunMetadata(cfg.name, cfg.run_id, cfg.metadata_path())
    layers = [layer.strip() for layer in args.layers.split(",")]

    print(f"== data-factory :: pipeline '{cfg.name}' | run {cfg.run_id}")
    print(f"   {cfg.description}")
    print(f"   camadas: {', '.join(layers)}")
    print(f"   source : {cfg.source_base}")

    spark = build_spark(f"data-factory-{cfg.name}", cfg.spark, repo_root=cfg.root)
    spark.sparkContext.setLogLevel("WARN")  # so as linhas da fabrica no console
    try:
        if "bronze" in layers:
            print("-- bronze")
            ingest_bronze(spark, cfg, meta)
            for table_cfg in cfg.tables:
                _quality_gate(spark, cfg, meta, "bronze", table_cfg["name"])
        if "silver" in layers:
            print("-- silver")
            transform_silver(spark, cfg, meta)
            for table_cfg in cfg.tables:
                _quality_gate(spark, cfg, meta, "silver", table_cfg["name"])
        if "gold" in layers:
            print("-- gold")
            build_gold(spark, cfg, meta)
            for gold_cfg in cfg.gold:
                _quality_gate(spark, cfg, meta, "gold", gold_cfg["name"])
    finally:
        spark.stop()

    report = meta.save()
    print(f"== run concluido | metadata: {report.relative_to(cfg.root)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="factory",
        description="Data Factory - pipelines Medallion (Bronze/Silver/Gold) com Delta Lake",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="executa um pipeline")
    p_run.add_argument("pipeline", help="nome do pipeline (arquivo YAML em config/pipelines)")
    p_run.add_argument("--layers", default="bronze,silver,gold", help="camadas a executar (default: todas)")
    p_run.add_argument("--source-base", default=None, help="override do caminho do source de dados")
    p_run.add_argument("--repo", default=None, help="caminho do repositorio (default: pai do pacote factory)")
    p_run.set_defaults(func=cmd_run)

    p_list = sub.add_parser("list", help="lista os pipelines disponiveis")
    p_list.add_argument("--repo", default=None)
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args(argv)
    args.func(args)
    return 0
