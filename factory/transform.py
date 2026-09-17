"""Camada Silver: limpeza, tipagem e renomeacao declaradas no YAML.

Aqui comeca a ficar "usavel", mas ainda sem regra de negocio de produto:
tipos corretos, nomes padronizados, filtros estruturais. Regras de negocio
analiticas sao da camada Gold.
"""

from __future__ import annotations

from time import perf_counter

from pyspark.sql import SparkSession, functions as F

from factory.storage import get_storage


def apply_silver(df: Any, silver_cfg: dict) -> Any:
    """Aplica as transformacoes Silver declaradas no YAML, nesta ordem:
    explode -> select -> rename -> casts -> where -> dedup -> drop.
    """
    explode_cfg = silver_cfg.get("explode")
    if explode_cfg:
        # Ex.: explode: {column: data, alias: media} -> uma linha por item do array.
        df = df.withColumn(explode_cfg["alias"], F.explode(F.col(explode_cfg["column"])))
    if "select" in silver_cfg:
        df = df.selectExpr(*silver_cfg["select"])
    for old, new in silver_cfg.get("rename", {}).items():
        df = df.withColumnRenamed(old, new)
    for col, ctype in silver_cfg.get("casts", {}).items():
        df = df.withColumn(col, F.col(col).cast(ctype))
    if "where" in silver_cfg:
        df = df.filter(silver_cfg["where"])
    dedup_cols = silver_cfg.get("dedup")
    if dedup_cols:
        df = df.dropDuplicates(dedup_cols)
    for col in silver_cfg.get("drop", []):
        if col in df.columns:
            df = df.drop(col)
    return df


def transform_silver(spark: SparkSession, cfg: Any, meta: Any) -> dict[str, int]:
    """Le Bronze, aplica transformacoes Silver e escreve lake/silver/<pipeline>/<table>."""
    storage = get_storage()
    counts: dict[str, int] = {}

    for table_cfg in cfg.tables:
        name = table_cfg["name"]
        silver_cfg = table_cfg.get("silver", {})
        if not silver_cfg:
            continue  # tabela sem transformacao Silver configurada
        t0 = perf_counter()
        df = spark.read.format("delta").load(storage.delta_uri(cfg.lake_path("bronze", name)))
        df = apply_silver(df, silver_cfg)
        out = cfg.lake_path("silver", name)
        df.write.format("delta").mode("overwrite").save(storage.delta_uri(out))
        n = df.count()
        counts[name] = n
        meta.add_layer("silver", name, n, perf_counter() - t0)
        print(f"  [silver] {name}: {n} linhas -> {out.relative_to(cfg.root)}")
    return counts
