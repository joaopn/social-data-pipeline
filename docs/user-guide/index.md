---
title: User guide
nav_order: 2
has_children: true
---
# User guide

How to install and use Social Data Pipeline, from adding a data source to querying the databases. If you are new, read [Install](install.md), then [Quick start](quick-start.md), then the page for your data ([Reddit dumps](reddit.md) or [Your own data and Hugging Face](custom-data.md)).

| Page | What it covers |
|---|---|
| [Install](install.md) | Requirements, installing the CLI, the Codespaces demo, and updating |
| [Quick start](quick-start.md) | A first end-to-end run: configure, run the pipeline, query, and what lands on disk |
| [Reddit dumps](reddit.md) | Processing Reddit data dumps from Arctic Shift |
| [Your own data and Hugging Face](custom-data.md) | Adding your own JSON, CSV or Parquet data and Hugging Face datasets as a platform |
| [Parsing](parsing.md) | The parse profile: decompressing dumps and parsing them into typed files |
| [Language detection and classifiers](classifiers.md) | Lingua language detection and the GPU classifiers |
| [Custom classifiers](custom-classifiers.md) | Adding your own classifier |
| [Databases](databases.md) | Ingesting into PostgreSQL, MongoDB and StarRocks, and choosing between them |
| [AI access](ai-access.md) | The read-only MCP servers that let AI agents query the databases |
| [Query scheduler](query-scheduler.md) | The jobs profile: agents queue queries, you approve them, results land as files |
| [Web UI](webui.md) | The browser page for the pipeline of one deployment, and the broker behind it |
| [Chat](chat.md) | An AI assistant in a sandbox that answers questions about your data |
| [Configuration reference](configuration.md) | Every environment variable, YAML file and source override |
| [CLI reference](cli.md) | Every `sdp.py` command and flag |
| [FAQ and troubleshooting](faq.md) | Common questions, and what to run when something fails |
