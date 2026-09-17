"""Camada Bronze: ingestao bruta do source para Delta, sem transformar valores.

Principio: Bronze preserva o dado como o source manda (todas as colunas como string),
adicionando apenas colunas tecnicas (_ingested_at, _source_file).

A config permite extracao seletiva por dominio:
- `filter`: expressao SQL aplicada na leitura (ex.: so os episodios de uma serie);
- `join_filter`: semi-join com outra tabela ja ingerida nesta rodada;
- `select`: projecao de colunas (sem alterar valores).

Isso e EL, nao T: nenhuma regra de negocio entra aqui.
"""

from __future__ import annotations

from time import perf_counter

from pyspark.sql import DataFrame, SparkSession, functions as F

from factory.storage import get_storage


def _reader(spark: SparkSession, fmt: str) -> Any:
    if fmt == "tsv_gz":
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("sep", "\t")
            .option("inferSchema", "false")
            .option("quote", '"')
            .option("escape", '"')
            .option("encoding", "UTF-8")
        )
    if fmt == "csv":
        return spark.read.format("csv").option("header", "true").option("inferSchema", "false")
    if fmt == "json":
        return spark.read.format("json")
    raise ValueError(f"formato de source desconhecido: {fmt!r}")


def load_source(spark: SparkSession, cfg: Any, table_cfg: dict, already_loaded: dict[str, DataFrame]) -> DataFrame:
    """Le um arquivo do source e aplica a extracao seletiva configurada."""
    path = cfg.source_path(table_cfg["file"])
    fmt = table_cfg.get("format", "tsv_gz")

    reader = _reader(spark, fmt)
    for key, value in table_cfg.get("options", {}).items():
        reader = reader.option(key, value)

    df = reader.load(path.as_posix())

    if "filter" in table_cfg:
        df = df.filter(table_cfg["filter"])
    if "select" in table_cfg:
        df = df.selectExpr(*table_cfg["select"])
    join_filter = table_cfg.get("join_filter")
    if join_filter:
        ref = already_loaded[join_filter["table"]]
        df = df.join(ref.select(F.col(join_filter["column"])), join_filter["column"], "inner")
    return df


def ingest_bronze(spark: SparkSession, cfg: Any, meta: Any) -> dict[str, int]:
    """Escreve cada tabela do source em lake/bronze/<pipeline>/<table> (Delta, overwrite)."""
    storage = get_storage()
    counts: dict[str, int] = {}
    already_loaded: dict[str, DataFrame] = {}

    for table_cfg in cfg.tables:
        name = table_cfg["name"]
        t0 = perf_counter()
        df = load_source(spark, cfg, table_cfg, already_loaded)
        df = df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
            "_source_file", F.lit(table_cfg["file"])
        )
        out = cfg.lake_path("bronze", name)
        df.write.format("delta").mode("overwrite").save(storage.delta_uri(out))
        n = spark.read.format("delta").load(storage.delta_uri(out)).count()
        counts[name] = n
        already_loaded[name] = df.drop("_ingested_at", "_source_file")
        meta.add_layer("bronze", name, n, perf_counter() - t0)
        print(f"  [bronze] {name}: {n} linhas -> {out.relative_to(cfg.root)}")
    return counts
