"""Abstracao de storage do lake.

Hoje: sistema de arquivos local (file://). Em breve: MinIO/S3 (s3a://).
O objetivo e que o resto da fabrica nunca construa URIs manualmente.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class Storage(ABC):
    """Interface de acesso ao lake."""

    @abstractmethod
    def delta_uri(self, path: Path) -> str:
        """URI usada pelo Spark para ler/escrever Delta neste storage."""


class LocalStorage(Storage):
    """Storage local (fase 0)."""

    def delta_uri(self, path: Path) -> str:
        return "file:///" + path.resolve().as_posix()


def get_storage(kind: str = "local") -> Storage:
    """Factory de storage. `kind` vem da config do pipeline (futuro: `s3`)."""
    if kind == "local":
        return LocalStorage()
    raise ValueError(f"storage nao suportado: {kind!r} (disponivel: 'local')")
