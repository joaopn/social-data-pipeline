---
title: "Quick start"
parent: "User guide"
nav_order: 3
---
# Quick start

This page takes you through a first end-to-end run: configure the databases and a source, process the dumps, ingest them, and query the result. It assumes you have [installed](install.md) the CLI and have Docker Compose v2.

## The steps

**1. Configure the databases (one time).** `db setup` walks you through database selection (PostgreSQL, MongoDB, StarRocks), data paths, port choices, optional authentication, and per-database tuning (PGTune for PostgreSQL, the FE/BE memory split for StarRocks). Not sure which database? See [Choosing a database](databases.md#choosing-a-database).

```bash
python sdp.py db setup                      # databases, with optional auth
python sdp.py db setup --add starrocks      # later: add one database without reconfiguring the others
```

**2. Optionally, set up agentic access.** The read-only MCP servers let AI agents query the databases; the query scheduler queues their larger queries for your approval. Both layer on top of `db setup` and reuse its read-only credentials; they are optional but recommended for agentic workflows. See [AI access](ai-access.md) and [Query scheduler](query-scheduler.md).

```bash
python sdp.py db setup-mcp                  # read-only MCP servers for agentic clients
python sdp.py db setup-jobs                 # query scheduler: page + MCP for human-approved agent queries
python sdp.py db setup-llm                  # or both at once: setup-mcp, then setup-jobs
```

**3. Add a data source.** `source add` walks you through data types, file patterns, fields, indexes, and classifier configuration, and writes the source's configuration to `config/sources/<name>/`. The name `reddit` uses the Reddit platform; any other name is a platform of your own (see [Reddit dumps](reddit.md) and [Your own data and Hugging Face](custom-data.md)).

```bash
python sdp.py source add reddit             # interactive setup
```

**4. Put the data in place.** For Reddit, download the data dumps from [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift/blob/master/download_links.md) and place them in the dumps directory you chose during setup (by default `data/dumps/reddit/submissions/` and `data/dumps/reddit/comments/`). For Hugging Face datasets, `python sdp.py source download <name>` fetches the files (see [Your own data and Hugging Face](custom-data.md)).

**5. Start the databases.** `db start` brings up everything `db setup`, `setup-mcp` and `setup-jobs` configured.

```bash
python sdp.py db start                      # databases + MCP servers + scheduler, whatever is configured
```

**6. Run the pipeline.** Each command runs one profile in its own container. The `--source` flag is optional when only one source is configured.

```bash
python sdp.py run parse --source reddit       # decompress dumps, parse to structured files
python sdp.py run lingua --source reddit      # optional: language detection (if configured during source add)
python sdp.py run ml --source reddit          # optional: GPU transformer classifiers (toxicity, emotions, ...)
python sdp.py run sr_ingest --source reddit   # ingest parsed files into StarRocks (or postgres_ingest / mongo_ingest)
python sdp.py run sr_ml --source reddit       # optional: ingest classifier outputs into StarRocks (or postgres_ml)
```

The optional steps have an order:

| Step | When to run it | Why |
|---|---|---|
| `lingua` | Before `*_ingest` | So the language columns fold into the main table |
| `ml` | Any time after `parse` | It writes independent output files |
| `*_ml` | After its `*_ingest` counterpart | Classifier tables foreign-key into the main table |

See [Language detection and classifiers](classifiers.md) for tuning. Every profile tracks its progress and resumes automatically: rerun any profile safely without reprocessing completed files.

**7. Check where things stand.**

```bash
python sdp.py db status                     # database config, health, MCP servers, scheduler
python sdp.py db settings                   # the current answers of db setup
python sdp.py source status                 # ingestion state per source
python sdp.py source progress               # files finished per processing stage
python sdp.py source size                   # disk space of each stage folder per source
python sdp.py runs list                     # pipeline runs
python sdp.py runs logs <run_id>            # output of a run (--follow to keep reading)
python sdp.py runs cancel <run_id>          # stop a run with the interrupt signal
python sdp.py source error-logs             # error details for failed datasets
python sdp.py db verify                     # checks config, credentials, mounts for consistency
```

**8. Query the data.** There are three ways to use it:

- **Directly**, with any standard client:
  - **PostgreSQL**: [psql](https://www.postgresql.org/docs/current/app-psql.html), [pgAdmin](https://www.pgadmin.org/), or [DBeaver](https://dbeaver.io/).
  - **StarRocks**: any MySQL client (`mysql -h 127.0.0.1 -P 9030 -u root`), or one of a growing list of [IDEs with official support](https://docs.starrocks.io/docs/cover_pages/ide_tools/).
  - **MongoDB**: `mongosh`, [MongoDB Compass](https://www.mongodb.com/products/tools/compass).
- **Agentic discovery (read-only MCP)**: short, ad-hoc questions from an AI agent. The per-database MCP servers (started automatically alongside their database) expose read-only access to agentic clients like Claude Code, Codex, and Cursor. They are best for quick queries to explore the dataset and schema, and to help the agent craft complex analysis queries. Configured with `db setup-mcp`; see [AI access](ai-access.md).
- **Agentic execution (query scheduler)**: human-approved queries that produce result files. Agents submit queries over the scheduler's MCP endpoint at `http://localhost:8050/mcp`; the page at `http://localhost:8050/` lets you approve, reject or kill pending jobs, EXPLAIN them against the live backend, and preview completed results inline with one-click download. Result files (Parquet, CSV or NDJSON) land in `JOBS_RESULT_ROOT/<job_id>/`, keeping long-running aggregations out of the agent's context. An opt-in per-target auto-accept switch (with a FIFO cap) is available for trusted workloads. Configured with `db setup-jobs`; see [Query scheduler](query-scheduler.md).

**9. Optionally, open the web UI and the chat.** The web UI is a page in your browser that works through the broker, a host process; the chat is an AI assistant in a sandbox that you ask in the web UI's Chat window. See [Web UI](webui.md) and [Chat](chat.md).

```bash
python sdp.py broker passwd                 # set the management password
python sdp.py broker start                  # start the broker
python sdp.py chat setup                    # back end, credential (not echoed), work folder's cap
python sdp.py chat start                    # start its gateway and sandbox
python sdp.py webui setup                   # port, addresses, names, certificate; builds its image
python sdp.py webui start                   # start the page
```

## Profiles

Each pipeline stage and database is a Docker Compose profile. 12 profiles.

| Profile | Description | Input | Output |
|---|---|---|---|
| `parse` | Decompress dumps, parse JSON/CSV/Parquet to Parquet/CSV | Compressed dump files, extracted JSON/CSV/Parquet | `PARSED_PATH/` |
| `lingua` | Lingua language detection (CPU) | Parsed files | `OUTPUT_PATH/lingua/` |
| `ml` | Transformer classifiers (GPU) | Parsed files + Lingua output | `OUTPUT_PATH/{classifier}/` |
| `postgres` | PostgreSQL database server | — | — |
| `postgres_ingest` | Ingest into PostgreSQL | Parsed files (or Lingua-enriched) | PostgreSQL tables |
| `postgres_ml` | Ingest ML outputs into PostgreSQL | Classifier output files | PostgreSQL tables |
| `mongo` | MongoDB database server | — | — |
| `mongo_ingest` | Ingest raw data into MongoDB | Extracted JSON/NDJSON/CSV/Parquet | MongoDB collections |
| `starrocks` | StarRocks OLAP database server | — | — |
| `sr_ingest` | Ingest parsed files into StarRocks | Parsed files (or Lingua-enriched) | StarRocks tables |
| `sr_ml` | Ingest ML outputs into StarRocks | Classifier output files | StarRocks tables |
| `jobs` | Query scheduler: page + MCP for human-approved agent queries against PostgreSQL, MongoDB and StarRocks | SQL/aggregation submissions | `JOBS_RESULT_ROOT/<job_id>/` |

`python sdp.py run` takes the eight pipeline profiles (`parse`, `lingua`, `ml`, `postgres_ingest`, `postgres_ml`, `mongo_ingest`, `sr_ingest`, `sr_ml`). The database servers and `jobs` start with `python sdp.py db start`. The GPU profile requires the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html).

## What you get on disk

With the default paths, every stage writes into its own folder under `data/`, one subfolder per source and data type. The example is a `reddit` source with one month of comments.

```
data/
├── dumps/reddit/comments/RC_2024-01.zst                        # what you downloaded, 1-to-1 with the origin
├── extracted/reddit/comments/RC_2024-01                        # parse: decompressed records
├── parsed/reddit/comments/RC_2024-01.parquet                   # parse: selected fields, typed
├── output/reddit/
│   ├── lingua/comments/RC_2024-01_lingua.parquet               # lingua: parsed records + language columns
│   └── toxic_roberta/comments/RC_2024-01_toxicity_en.parquet   # ml: one folder per classifier
├── database/
│   ├── postgres/                                               # PostgreSQL's data folder
│   ├── mongo/                                                  # MongoDB's data folder
│   └── starrocks/                                              # StarRocks's data folder
└── jobs/                                                       # the query scheduler's job store
    ├── pending/ approved/ running/ history/                    # one JSON file per job, by phase
    ├── control/                                                # its control socket (for the web UI)
    └── results/<job_id>/                                       # result files of each query
```

The paths are set per source during `source add` (dumps, extracted, parsed, output) and per database during `db setup`; the results folder is `JOBS_RESULT_ROOT`. To reprocess, delete a stage's output and rerun its profile (see [FAQ](faq.md#how-do-i-reprocess-data)).

## Where to go next

- **Your data:** [Reddit dumps](reddit.md), or [Your own data and Hugging Face](custom-data.md).
- **The stages:** [Parsing](parsing.md), [Language detection and classifiers](classifiers.md), [Databases](databases.md).
- **Agents:** [AI access](ai-access.md) and [Query scheduler](query-scheduler.md).
- **Every command:** [CLI reference](cli.md); every setting: [Configuration reference](configuration.md).
