"""Carregamento e resolucao da configuracao de um pipeline (YAML)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


@dataclass
class PipelineConfig:
    """Configuracao de um pipeline de dominio, declarada em config/pipelines/<nome>.yaml."""

    name: str
    description: str = ""
    source_base: str = ""
    tables: list[dict[str, Any]] = field(default_factory=list)
    gold: list[dict[str, Any]] = field(default_factory=list)
    checks: list[dict[str, Any]] = field(default_factory=list)
    spark: dict[str, Any] = field(default_factory=dict)
    abort_on_fail: bool = True
    yaml_path: Path | None = None

    # Resolvidos em runtime
    root: Path = field(default_factory=Path)
    run_id: str = ""

    @classmethod
    def load(cls, yaml_path: Path, source_base: str | None = None) -> "PipelineConfig":
        raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        quality = raw.get("quality") or {}
        cfg = cls(
            name=raw.get("pipeline", yaml_path.stem),
            description=raw.get("description", ""),
            source_base=source_base or raw.get("source_base", ""),
            tables=raw.get("tables", []),
            gold=raw.get("gold", []),
            checks=raw.get("checks", []),
            spark=raw.get("spark", {}),
            abort_on_fail=quality.get("abort_on_fail", True),
            yaml_path=yaml_path,
        )
        # <repo>/config/pipelines/<nome>.yaml -> <repo>
        cfg.root = yaml_path.resolve().parents[2]
        cfg.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return cfg

    def source_path(self, rel: str) -> Path:
        """Caminho de um arquivo do source (relativo ao source_base)."""
        return Path(self.source_base) / rel if self.source_base else Path(rel)

    def lake_path(self, layer: str, table: str | None = None) -> Path:
        """Caminho de uma tabela no lake: lake/<layer>/<pipeline>/<table>."""
        p = self.root / "lake" / layer / self.name
        return p / table if table else p

    def metadata_path(self) -> Path:
        """Diretorio de metadata desta execucao."""
        return self.root / "lake" / "metadata" / self.name / self.run_id
