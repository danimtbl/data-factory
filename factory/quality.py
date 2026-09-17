"""Data quality: checks declarados no YAML do pipeline, executados sobre tabelas do lake.

Tipos suportados:
- not_empty          -> a tabela precisa ter pelo menos 1 linha
- not_null           -> coluna sem nulos
- unique             -> combinacao de colunas sem duplicatas
- range              -> min/max da coluna dentro de [min, max]
- max_null_fraction  -> fracao de nulos abaixo de `limit`
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import functions as F


@dataclass
class CheckResult:
    table: str
    layer: str
    check: dict[str, Any]
    passed: bool
    detail: dict[str, Any]


def run_checks(spark: Any, df: Any, table: str, layer: str, checks: list[dict[str, Any]]) -> list[CheckResult]:
    """Aplica a lista de checks sobre o DataFrame e retorna o resultado de cada um."""
    results: list[CheckResult] = []
    for check in checks:
        ctype = check.get("type")
        detail: dict[str, Any] = {"rule": ctype}
        try:
            if ctype == "not_empty":
                rows = df.count()
                passed = rows > 0
                detail["rows"] = rows
            elif ctype == "not_null":
                col = check["column"]
                nulls = df.filter(F.col(col).isNull()).count()
                passed = nulls == 0
                detail.update(column=col, nulls=nulls)
            elif ctype == "unique":
                cols = check["columns"]
                total = df.count()
                distinct = df.select(*cols).distinct().count()
                passed = total == distinct
                detail.update(columns=cols, rows=total, distinct=distinct)
            elif ctype == "range":
                col = check["column"]
                row = df.agg(F.min(col).alias("lo"), F.max(col).alias("hi")).first()
                lo, hi = row["lo"], row["hi"]
                passed = (
                    lo is not None
                    and hi is not None
                    and lo >= check.get("min", float("-inf"))
                    and hi <= check.get("max", float("inf"))
                )
                detail.update(column=col, min=lo, max=hi)
            elif ctype == "max_null_fraction":
                col = check["column"]
                total = df.count()
                nulls = df.filter(F.col(col).isNull()).count()
                frac = nulls / total if total else 1.0
                passed = frac <= check["limit"]
                detail.update(column=col, null_fraction=round(frac, 6), limit=check["limit"])
            else:
                raise ValueError(f"tipo de check desconhecido: {ctype!r}")
        except Exception as exc:  # noqa: BLE001 - qualquer falha vira FAIL explicito
            passed = False
            detail["error"] = str(exc)
        results.append(CheckResult(table=table, layer=layer, check=check, passed=passed, detail=detail))
    return results
