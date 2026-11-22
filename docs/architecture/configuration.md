---
title: "Setup and configuration"
parent: "Architecture"
nav_order: 2
---
# Setup and configuration

The setup commands (`python sdp.py db setup`, `python sdp.py source add`) ask the questions and write the config files; inside the containers, each profile loads its base config from `config/<profile>/` and lays the source's override file from `config/sources/<name>/` over it. This page describes how those layers merge. Every setting and file is listed in [Configuration reference](../user-guide/configuration.md).

## How the layers merge

1. **Base config.** The profile's base files are loaded from its config folder: `config/<profile>/pipeline.yaml`, plus the other profile-level files (`cpu_classifiers.yaml` for `lingua`, `gpu_classifiers.yaml` for `ml`, `services.yaml` for `postgres_ml` and `sr_ml`). A missing base file is an error.
2. **Source override.** The source's override file for that profile, `config/sources/<name>/<file>.yaml`, is deep-merged on top.
3. **Scoped by file name.** Overrides are scoped by config filename: each top-level key of the override file corresponds to one base file, without the `.yaml` extension (`pipeline:` → `pipeline.yaml`, `gpu_classifiers:` → `gpu_classifiers.yaml`, `services:` → `services.yaml`). A top-level key that names no base file of that profile is not applied.
4. **Lists replace.** A list value in an override fully replaces the base list; lists are never merged. Mappings are merged key by key, and the override's value wins.
5. **Multi-file profiles.** The base files of one profile are merged in a fixed order (`pipeline.yaml` first), and a later file wins on a shared key.

The profile folders and override files do not always share a name:

| Profile | Base folder | Base files | Source override file |
|---------|-------------|------------|----------------------|
| `parse` | `config/parse/` | `pipeline.yaml` | `parse.yaml` |
| `lingua` | `config/lingua/` | `pipeline.yaml`, `cpu_classifiers.yaml` | `lingua.yaml` |
| `ml` | `config/ml/` | `pipeline.yaml`, `gpu_classifiers.yaml` | `ml.yaml` |
| `postgres_ingest` | `config/postgres/` | `pipeline.yaml` | `postgres.yaml` |
| `postgres_ml` | `config/postgres_ml/` | `pipeline.yaml`, `services.yaml` | `postgres_ml.yaml` |
| `mongo_ingest` | `config/mongo/` | `pipeline.yaml` | `mongo.yaml` |
| `sr_ingest` | `config/sr/` | `pipeline.yaml` | `starrocks.yaml` |
| `sr_ml` | `config/sr_ml/` | `pipeline.yaml`, `services.yaml` | `sr_ml.yaml` |

`config/starrocks/` is not a profile config folder: it holds the StarRocks server's config templates.

The platform config, `config/sources/<name>/platform.yaml`, is not merged with anything: it is one self-contained file per source.

## Environment overrides

After the merge, a few environment variables from `.env` override database keys:

| Profile | Variable | Overrides |
|---------|----------|-----------|
| `postgres_ingest`, `postgres_ml` | `POSTGRES_PORT`, `DB_NAME` | `database.port`, `database.name` |
| `postgres_ingest`, `postgres_ml` | `POSTGRES_PASSWORD` (prompted at start) | The connection password |
| `mongo_ingest` | `MONGO_PORT` | `database.port` |
| `mongo_ingest` | `MONGO_ADMIN_USER`, `MONGO_ADMIN_PASSWORD` (prompted at start) | `database.user`, the connection password |
| `sr_ingest`, `sr_ml` | `STARROCKS_PORT`, `STARROCKS_FE_HTTP_PORT` | `database.port`, `database.fe_http_port` |
| `sr_ingest`, `sr_ml` | `STARROCKS_ROOT_PASSWORD` (prompted at start) | The connection password |
