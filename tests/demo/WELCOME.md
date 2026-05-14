<div align="center">

# Social Data Pipeline

[![Docker](https://img.shields.io/badge/Docker-Compose_v2-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL 18](https://img.shields.io/badge/PostgreSQL-18-4169E1.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MongoDB 8](https://img.shields.io/badge/MongoDB-8-47A248.svg?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![StarRocks](https://img.shields.io/badge/StarRocks-OLAP-FF6D00.svg?logo=starrocks&logoColor=white)](https://www.starrocks.io/)

### Codespace demo



</div>



You're operating a working SDP install. PostgreSQL, MongoDB, and StarRocks are all pre-configured (along with their read-only MCP servers and the jobs scheduler). Nothing is running yet — pick a backend and start it.

> The codespace pipx-installs the CLI for you, so `sdp <command>` works directly. The main [README](../../README.md) uses `python sdp.py <command>` instead — they are identical.

## 1. Pick a database and start it

PostgreSQL is the recommended default for the free-tier Codespace (2 cores / 8 GB).

```bash
sdp db start postgres
```

This installs and brings up PostgreSQL **plus** the PostgreSQL read-only MCP (port 8000) **plus** the jobs scheduler (port 8050) — they're auto-bundled with the database. Takes 30-60 seconds.

### Alternatives

```bash
sdp db start mongo        # Mongo + mongo-mcp (port 3000) + jobs
sdp db start starrocks    # StarRocks + sr-mcp (port 9000) + jobs (heavier; see Limits)
```

> **Don't run bare `sdp db start`** on the free tier — that launches all three backends at once and will likely OOM the 8 GB box.

## 2. Bring data

### Path A · Reddit dump

Drop `.zst` files from [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift) into the right subdirectory:

- `RS_YYYY-MM.zst` (submissions) → `data/dumps/reddit/submissions/`
- `RC_YYYY-MM.zst` (comments) → `data/dumps/reddit/comments/`

Small monthly files (10–100 MB) recommended due to storage and upload limits.

```bash
sdp run parse              # decompress + parse to Parquet
sdp run lingua             # per-row language detection
sdp run postgres_ingest    # or: mongo_ingest, sr_ingest — pick what you started
```

> **Skipping lingua?** Lingua can take minutes on only 2 cores and the base source is pre-configured to ingest its output. To skip it, either edit `config/sources/reddit/postgres.yaml` and set `prefer_lingua: false`, or re-run `sdp source configure reddit` and answer "no" to the lingua-prefer prompt. Then `sdp run postgres_ingest` ingests directly from `parsed/`, with no `lang` / `lang_prob` columns in the table.

### Path B · HuggingFace dataset (auto-configured)

`sdp source add --hf` queries the HF dataset API and pre-fills the platform config. Two datasets to try (multilingual, permissive licenses):

- `cardiffnlp/tweet_sentiment_multilingual` — 24k tweets, 8 languages, ~4 MB
- `wikimedia/wikipedia` configs `20231101.eu` + `20231101.simple` — 640k rows, 2 languages, ~400 MB

```bash
sdp source add tweets --hf cardiffnlp/tweet_sentiment_multilingual
sdp source download tweets
sdp run parse --source tweets
sdp run postgres_ingest --source tweets     # or mongo_ingest / sr_ingest
```

## 3. Agentic query via Copilot Chat

On the **Copilot Chat** sidebar on the right, click on the `Configure Tools` icon and enable the relevant MCP servers: 

- `sdp-jobs` — submits queries through the approval queue (any DB you started)
- `sdp-postgres` / `sdp-mongo` / `sdp-starrocks` — direct read-only access to whichever DB is running, for schema lookups and quick queries

You must first click on "Update Tools" for each. On first tool usage the LLM can ask "Allow tool?". Click allow.

Some queries to test with the reddit data:


> Using the sdp database MCP (not sdp-jobs), give me a summary of the data. What do the fields mean?

> Using the sdp database MCP (not sdp-jobs), find me the top 10 most upvoted submissions and display them. Group them in topics

> Use the sdp database MCP to understand the schema. Then submit a sdp-jobs query for total submissions, comments, total score, and average score of all subreddits ranked by score

> Use the sdp database MCP to understand the schema. Then submit separate sdp-jobs queries for count and total score of all submission authors and domains, ranked by score


## 4. Approve the query in the WebUI

Open the jobs scheduler UI on port 8050 — either:

- Run this in the terminal:
  ```bash
  "$BROWSER" "https://${CODESPACE_NAME}-8050.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}/"
  ```
- Or open the **Ports** panel (bottom of VS Code) → click the globe icon on port 8050.

Submitted queries appear as `pending`. From the UI you can:

- **Approve / Reject / Kill** any pending or running job.
- **Auto-accept** (per-target toggle) — bypass the approval queue for the duration of the session when you trust what Copilot is sending.
- **EXPLAIN** any pending query — runs the backend's planner (`EXPLAIN` for PG/SR, `aggregate explain` for Mongo) against the SQL/pipeline and shows the plan inline, so you can sanity-check what Copilot drafted before approving.
- **Preview** a completed result inline — first N rows rendered as a table in the UI. Full result files land under `data/jobs-results/` and download from the same row.

## Limits and notes

- **Free Codespace tier:** 2 cores / 8 GB RAM / 32 GB disk / 60 core-hours per month — roughly 30 hours of wall time for this demo.
- **StarRocks is tuned aggressively low** (1 GB FE heap, 2 GB BE limit) to fit the free tier. Heavy queries may push it over budget; if SR crashes, restart with `sdp db stop starrocks && sdp db start starrocks` or use Postgres/Mongo instead.
- **Auth is off** — single-user demo, no passwords. Full setup with auth + read-only credentials is documented in the [main README](../../README.md).
