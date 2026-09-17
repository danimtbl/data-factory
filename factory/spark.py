"""Criacao da SparkSession local com Delta Lake habilitado."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _ensure_java_home() -> None:
    """No-op de seguranca: o PySpark resolve o Java pelo PATH.

    (Nao setamos JAVA_HOME automaticamente: nesta maquina o java do PATH vem de
    C:\\Program Files\\Common Files\\Oracle\\Java\\javapath e o caminho com espacos
    quebra o comando de launch do gateway quando embutido em JAVA_HOME.
    O `java` do PATH funciona sem problema.)
    """
    return


def _ensure_hadoop_home(repo_root: Path) -> str | None:
    """No Windows o Spark precisa do winutils.exe/hadoop.dll para operacoes de IO.

    Procura binarios do Hadoop em (1) <repo>/tools/hadoop e (2) <repo>/../../tools/hadoop
    (padrao deste portfolio: F:/portfolio/tools/hadoop). Veja scripts/setup_hadoop.ps1.

    Alem de setar HADOOP_HOME, adiciona a pasta bin ao PATH do processo e devolve
    o caminho dela (usado no -Djava.library.path do driver).
    """
    if os.name != "nt":
        return None
    candidates = [
        repo_root / "tools" / "hadoop",
        repo_root.parents[1] / "tools" / "hadoop",
    ]
    for candidate in candidates:
        if (candidate / "bin" / "winutils.exe").exists():
            bin_dir = str(candidate / "bin")
            os.environ["HADOOP_HOME"] = str(candidate)
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
            return bin_dir
    # Se HADOOP_HOME ja esta definido no ambiente, confia nele.
    if os.environ.get("HADOOP_HOME"):
        return str(Path(os.environ["HADOOP_HOME"]) / "bin")
    return None


def build_spark(app_name: str, config: dict[str, Any] | None = None, repo_root: Path | None = None) -> Any:
    """SparkSession local com Delta Lake configurado.

    Args:
        app_name: nome do aplicativo Spark.
        config: configuracoes extras (spark.*) vindas do YAML do pipeline.
        repo_root: raiz do repositorio (para localizar binarios do Hadoop no Windows).
    """
    from pyspark.sql import SparkSession

    hadoop_bin = _ensure_hadoop_home(repo_root) if repo_root is not None else None
    _ensure_java_home()

    # Os workers Python do Spark precisam do caminho absoluto do python do venv.
    # Sem isso, o Windows resolve "python" para o alias da Microsoft Store e o
    # worker morre com "Python worker failed to connect back".
    import sys

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "true")
    )
    for key, value in (config or {}).items():
        builder = builder.config(key, value)

    if hadoop_bin:
        # O gateway JVM do PySpark nasce ANTES das configs do builder (local mode),
        # entao spark.driver.extraJavaOptions nao chega nele. _JAVA_OPTIONS e lida
        # por qualquer JVM ao iniciar — sem isso o Delta falha com UnsatisfiedLinkError.
        java_opts = os.environ.get("_JAVA_OPTIONS", "")
        if "-Djava.library.path" not in java_opts:
            os.environ["_JAVA_OPTIONS"] = f"{java_opts} -Djava.library.path={hadoop_bin}".strip()

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:  # pragma: no cover - fallback manual de jars
        import delta

        jars = list(Path(delta.__file__).resolve().parent.glob("jars/*.jar"))
        builder = builder.config("spark.jars", ",".join(str(j) for j in jars))

    return builder.getOrCreate()
