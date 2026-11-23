---
title: "FAQ and troubleshooting"
parent: "User guide"
nav_order: 16
---
# FAQ and troubleshooting

Short answers to the common questions, and the first commands to run when something fails.

## Questions

### Should I use PostgreSQL or StarRocks?

Both ingest the same parsed files and support the same pipeline features (classifiers, lingua, MCP servers, authentication); use PostgreSQL for a general-purpose database that integrates with other tools, and StarRocks for fast analytical queries over large datasets. Both can be used in parallel; the comparison table is in [Choosing a database](databases.md#choosing-a-database).

### Can I run classifiers without the database?

Yes. Run `python sdp.py run lingua` or `python sdp.py run ml` on their own. The database profiles are optional.

### Can I use this for non-Reddit data?

Yes. Use any name other than the reserved `reddit` in `python sdp.py source add <name>` to process arbitrary NDJSON, CSV, or Parquet data. For Hugging Face datasets, use `python sdp.py source add <name> --hf <dataset_id>` to fetch the dataset's metadata and pre-populate the setup defaults. See [Your own data and Hugging Face](custom-data.md).

### How do I add custom support for a new platform?

Create a platform template in `config/templates/` and, optionally, a custom parser. See [Platforms](../architecture/platforms.md).

### How do I reprocess data?

Delete the relevant output folders and rerun the profile:

```bash
rm -rf data/output/<source>/toxic_roberta/                                   # reprocess a specific classifier
rm -rf data/output/<source>/                                                 # reprocess all classifiers
rm -rf data/output/<source>/ data/parsed/<source>/ data/extracted/<source>/  # full reprocess
```

To re-ingest one database from scratch, `python sdp.py db reset --db <postgres|mongo|starrocks>` wipes its data and ingestion state and keeps its configuration (see [CLI reference](cli.md#databases-python-sdppy-db)).

## Troubleshooting

### A pipeline stage fails

Read the stage's container log:

```bash
docker compose logs parse
docker compose logs lingua
docker compose logs postgres-ingest
docker compose logs sr-ingest
docker compose logs postgres-ml
docker compose logs sr-ml
```

`python sdp.py runs logs <run_id>` prints the output of a run that `python sdp.py runs list` shows, and `python sdp.py source error-logs` shows the error details of failed ingestion datasets.

### A database does not connect

```bash
docker compose ps
docker compose logs postgres
docker compose logs mongo
docker compose --profile starrocks logs starrocks
```

`python sdp.py db status` shows each database's health, and `python sdp.py db verify` checks config, credentials and mounts for drift.

### Out of disk space

- Use `cleanup_temp: true` in `pipeline.yaml`.
- Check the temp directories for leftover files.
- Consider sequential mode to reduce intermediate storage.

### The GPU is not detected

Check that Docker can reach the GPU through the NVIDIA Container Toolkit:

```bash
docker run --rm --gpus all nvidia/cuda:12.1.1-base-ubuntu22.04 nvidia-smi
```

### The web UI's Queue or Chat window is not available

`python sdp.py webui status` says which folders are mounted. Set up and start the scheduler or the chat first, then stop and start the web UI. The reasons and fixes are in [What the Queue window needs](webui.md#what-the-queue-window-needs) and [What the Chat window needs](webui.md#what-the-chat-window-needs).
