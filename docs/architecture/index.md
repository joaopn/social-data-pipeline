---
title: Architecture
nav_order: 3
has_children: true
---
# Architecture

How Social Data Pipeline works and why, for those who change or extend it.

| Page | What it covers |
|---|---|
| [Pipeline and data folders](pipeline.md) | The stages from a dump to a database, the data folders between them, and how a stage resumes |
| [Setup and configuration](configuration.md) | The configuration layers and which one wins |
| [Platforms](platforms.md) | The platform interface, the Reddit algorithms, Hugging Face downloads, and adding a platform |
| [Classifiers](classifiers.md) | How language detection and the GPU classifiers run, and custom Python classifiers |
| [Databases and ingestion](databases.md) | How each ingest profile loads, deduplicates and resumes |
| [Credentials and auth](credentials.md) | How database authentication works and where credentials live |
| [MCP servers](mcp.md) | How the MCP servers are built and kept read-only |
| [Query scheduler](query-scheduler.md) | The lifecycle of a job, concurrency, the control socket and the assistant's endpoint |
| [Broker, web UI and chat](control-plane.md) | How the broker, the web UI and the chat connect, and what they defend against |
| [Testing](testing.md) | The test suite and how to run it |
