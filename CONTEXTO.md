# CONTEXTO — Data Factory

> **Como retomar QUALQUER sessão:** "Leia F:\portfolio\projects\data-factory\CONTEXTO.md e me diga onde paramos — continue o projeto da Data Factory."
> Este arquivo é a fonte de verdade do projeto. **Atualizado em 2026-09-17.**

## O que é

Plataforma de dados reutilizável (Data Engineering) — a **infraestrutura central** do
portfólio "Dados da Dani". Ela não carrega regra de negócio de produto nenhum: é a camada
de engenharia/ingestão que os Data Products consomem (Supernatural, Anime & Manga,
The Sims, Educação em Foco...).

- **Arquitetura**: Medallion Bronze/Silver/Gold com PySpark + Delta Lake.
- **Data quality**: checks declarativos (YAML) com quality gate entre camadas.
- **Metadata**: cada run grava `lake/metadata/<pipeline>/<run_id>/run.json`
  (eventos por camada, contagens, durações, resultados de DQ) — base da observabilidade.

## Decisões tomadas (com a Dani, 2026-09-17)

1. **Stack local nativa** (sem Docker por ora): PySpark 3.5.5 + Delta Lake 3.2.1 em
   Python 3.11 dedicado (venv `.venv`). O Python 3.13 do sistema **não** é suportado
   pelo Spark; não mexer nele. Java 8 do sistema atende.
2. **Storage**: começa em sistema de arquivos local atrás da abstração `factory/storage.py`
   (`file:///`). Quando o MinIO entrar (fase 2), só muda o backend (`s3a://`) — o
   resto da fábrica não muda. Binários do Hadoop (winutils) ficam em
   `F:\portfolio\tools\hadoop\bin` (instalados via `scripts/setup_hadoop.ps1`).
3. **Orquestrador**: nasce **sem** — fase 0 usa a CLI própria
   (`python -m factory run <pipeline>`) com ordem explícita e quality gates.
   Dagster entra na fase 2 (roda bem no Windows; Airflow não é suportado nativo aqui).
4. **Bronze = EL, não T**: Bronze preserva o dado como o source manda (tudo string),
   mais colunas técnicas (`_ingested_at`, `_source_file`). A extração pode ser
   **seletiva por domínio** (`filter`, `join_filter` no YAML) — é extração, não
   transformação. Como os `.tsv.gz` do IMDb não permitem pushdown (gzip não é
   divisível), o filtro é aplicado logo após a leitura.
5. **Silver**: tipagem/renomeação/limpeza declaradas no YAML (select → rename →
   casts → where → drop). **Gold**: SQL declarado no YAML sobre views das tabelas Silver.
6. **Novo domínio = novo YAML**, zero código novo na fábrica. É isso que prova a
   agnosticidade (tese do projeto).
7. **Idempotência por overwrite** nas três camadas (full load). Incremental/CDC e
   merges (SCD) são roadmap.
8. **Git/GitHub**: repo público `danimtbl/data-factory` (criado 2026-09-17). O lake
   (`lake/`), venv e caches **não** são versionados — o código regenera tudo.

## Estrutura

```
data-factory/
  factory/            codigo generico (nao conhece dominio)
    cli.py            python -m factory run/list
    context.py        carrega e resolve o YAML do pipeline
    spark.py          SparkSession local + Delta + hadoop/winutils no Windows
    storage.py        abstracao de storage (local hoje, s3 depois)
    ingest.py         Bronze (leitura + extracao seletiva)
    transform.py      Silver (tipagem/renomeacao/limpeza)
    aggregate.py      Gold (SQL do YAML sobre views Silver)
    quality.py        checks DQ (not_empty, not_null, unique, range, max_null_fraction)
    metadata.py       run.json de cada execucao
  config/pipelines/   imdb.yaml (Supernatural), anilist.yaml (fase 1)
  lake/               bronze/silver/gold/<pipeline>/ + metadata/ (gitignored)
  scripts/            setup_hadoop.ps1
  tests/              pytest (config + quality com Spark local)
  README.md           vitrine do repo
  CONTEXTO.md         este arquivo
```

## Comandos (PowerShell, do repo)

```
.\.venv\Scripts\python.exe -m factory list
.\.venv\Scripts\python.exe -m factory run imdb                    # bronze+silver+gold
.\.venv\Scripts\python.exe -m factory run imdb --layers bronze
.\.venv\Scripts\python.exe -m factory run imdb --source-base "F:\outro\caminho"
.\.venv\Scripts\python.exe -m pytest tests -q
```

## Pipeline IMDb (caso Supernatural)

- **Source**: `F:\portfolio\projects\supernatural\data\raw` (datasets públicos IMDb).
- **Extração seletiva**: `title.episode` filtra a série (`tt0460681`); `title.basics` e
  `title.crew` entram por semi-join com os episódios; `title.ratings` (pequena) entra
  completa.
- **Silver**: `episode` (ids, temporada, número — tipados), `ratings` (nota/ votos),
  `basics` (títulos), `crew` (diretores/escritores por episódio).
- **Gold**: `episode_ratings` (episódio + título + nota + votos) e `season_summary`
  (por temporada: episódios, nota média, mediana, votos totais) — base dos insights
  P1–P5 do case.
- **Fora do escopo atual**: `name.basics` e `title.principals` (elenco/nomes) — exigem
  extração streaming (fase 3), pois são grandes demais para o padrão atual (gzip em
  partição única).

## Roadmap

- [x] **Fase 0** (2026-09-17) — núcleo da fábrica + pipeline IMDb fim-a-fim com DQ.
- [ ] **Fase 1** — pipeline `anilist` (anime-manga): JSON aninhado, segundo domínio.
- [ ] **Fase 2** — MinIO (S3 local) + Dagster (assets, lineage, schedules, retries).
- [ ] **Fase 3** — extração streaming para tabelas grandes do IMDb (name.basics,
      title.principals) → Gold de elenco/diretores com nomes.
- [ ] **Fase 4** — pipeline INEP/Censo Escolar (case Educação em Foco).
- [ ] **Site** — quando houver case: página do data-factory no portfólio (hoje o card
      existe na Home com status "planejado" e capa `menu_datafactory.png`).

## Regras do projeto

- Nunca inventar número/insight: tudo que a Gold produz vem dos dados reais do source.
- Toda execução é reproduzível a partir do YAML + código (dados não versionados).
- Quality gate aborta o run em FAIL (config `quality.abort_on_fail`).
- Mudanças na fábrica afetam todos os pipelines — mexer com teste; mudanças de domínio
  ficam no YAML.
- Documentar decisão de arquitetura aqui no CONTEXTO (com data).
