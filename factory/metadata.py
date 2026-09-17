"""Metadata de execucao: o que rodou, quando, quantas linhas e o resultado dos checks.

Cada run grava lake/metadata/<pipeline>/<run_id>/run.json. E a base da
observabilidade (e do lineage quando o Dagster entrar na fase 2).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RunMetadata:
    def __init__(self, pipeline: str, run_id: str, out_dir: Path):
        self.pipeline = pipeline
        self.run_id = run_id
        self.out_dir = Path(out_dir)
        self.events: list[dict[str, Any]] = []

    def add_layer(self, layer: str, table: str, rows: int, seconds: float, status: str = "ok", note: str | None = None) -> None:
        self.events.append(
            {
                "event": "layer",
                "layer": layer,
                "table": table,
                "rows": rows,
                "seconds": round(seconds, 2),
                "status": status,
                "note": note,
            }
        )

    def add_checks(self, layer: str, table: str, results: list[Any]) -> None:
        self.events.append(
            {
                "event": "quality",
                "layer": layer,
                "table": table,
                "checks": [
                    {"rule": r.check.get("type"), "passed": r.passed, "detail": r.detail}
                    for r in results
                ],
            }
        )

    def save(self) -> Path:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        path = self.out_dir / "run.json"
        payload = {
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "events": self.events,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
