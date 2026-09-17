# Data Factory

Plataforma de dados reutilizável — a infraestrutura central do portfólio **Dados da Dani**.

Pipeline Medallion (**Bronze → Silver → Gold**) com **PySpark + Delta Lake**, data quality
estruturada e metadata de execução. Agnóstica de domínio: um novo caso de uso entra como
um arquivo YAML, sem tocar no código da fábrica.

## Stack

- Python 3.11 + PySpark 3.5 + Delta Lake 3.2 (roda 100% local, sem Docker)
- Data Quality declarativa (YAML) com quality gate entre camadas
- Metadata de run em JSON (base para observabilidade/lineage)
- Fase 2 (roadmap): MinIO como S3 local + Dagster como orquestrador

## Quickstart

```powershell
# 1. ambiente (Python 3.11)
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 2. (Windows) binarios do Hadoop para o Spark
powershell -ExecutionPolicy Bypass -File scripts\setup_hadoop.ps1

# 3. rodar um pipeline
.\.venv\Scripts\python.exe -m factory list
.\.venv\Scripts\python.exe -m factory run imdb
```

Cada execução grava os dados em `lake/<camada>/<pipeline>/` e o relatório em
`lake/metadata/<pipeline>/<run_id>/run.json`.

## Estrutura

```
factory/           codigo generico da fabrica (nao conhece nenhum dominio)
config/pipelines/  um YAML por pipeline: source, tabelas, checks DQ e SQL Gold
lake/              data lake local (bronze/silver/gold/metadata) - nao versionado
scripts/           utilitarios (setup_hadoop.ps1)
tests/             testes dos modulos da fabrica
CONTEXTO.md        fonte de verdade do projeto (decisoes, roadmap, estado)
```

## Pipelines

- `imdb` — caso Supernatural: episódios e notas da série no IMDb, com agregados por temporada.
- `anilist` — caso Anime & Manga (fase 1).

## Roadmap

- [x] Fase 0 — núcleo da fábrica + pipeline IMDb (Supernatural) fim-a-fim
- [ ] Fase 1 — pipeline AniList (anime-manga): segundo domínio na mesma fábrica
- [ ] Fase 2 — MinIO (S3 local) via camada de storage + Dagster (assets, lineage, schedules)
- [ ] Fase 3 — extração streaming para as tabelas grandes do IMDb (name.basics, title.principals)
- [ ] Fase 4 — terceiro domínio (INEP/Censo Escolar → case Educação em Foco)
