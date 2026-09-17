"""Camada Gold: agregados analiticos declarados como SQL no YAML.

Cada tabela Gold e uma query sobre as tabelas Silver (views temporarias).
O SQL fica na config, nao no codigo: adicionar um dominio nao exige
mudar a fabrica.
"""

from __future__ import annotations

from time import perf_counter

from pyspark.sql import SparkSession

from factory.storage import get_storage


def _register_silver_views(spark: SparkSession, cfg: Any) -> None:
    storage = get_storage()
    for table_cfg in cfg.tables:
        name = table_cfg["name"]
        silver_path = cfg.lake_path("silver", name)
        df = spark.read.format("delta").load(storage.delta_uri(silver_path))
        df.createOrReplaceTempView(name)


def build_gold(spark: SparkSession, cfg: Any, meta: Any) -> dict[str, int]:
    """Executa as queries Gold e escreve lake/gold/<pipeline>/<tabela>."""
    storage = get_storage()
    counts: dict[str, int] = {}
    _register_silver_views(spark, cfg)

    for gold_cfg in cfg.gold:
        name = gold_cfg["name"]
        t0 = perf_counter()
        df = spark.sql(gold_cfg["sql"])
        out = cfg.lake_path("gold", name)
        df.write.format("delta").mode("overwrite").save(storage.delta_uri(out))
        n = df.count()
        counts[name] = n
        meta.add_layer("gold", name, n, perf_counter() - t0)
        print(f"  [gold] {name}: {n} linhas -> {out.relative_to(cfg.root)}")
    return counts
