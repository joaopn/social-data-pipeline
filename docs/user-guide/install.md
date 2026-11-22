---
title: "Install"
parent: "User guide"
nav_order: 1
---
# Install

This page gets the `sdp.py` CLI onto your machine, or into a browser-based demo, so you can configure databases and sources next. Everything else runs in Docker containers that the CLI builds and starts for you.

## Requirements

| What | Needed for |
|---|---|
| [Python](https://www.python.org/) 3.11+ | The `sdp.py` CLI and setup scripts |
| [PyYAML](https://pypi.org/project/PyYAML/) 6.0+ | The CLI reads and writes its YAML config with it (`pip install pyyaml`) |
| [Docker Compose](https://docs.docker.com/compose/) v2 | Every pipeline stage and database runs as a Docker Compose profile |
| [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) | GPU classification (the `ml` profile) only |
| Linux, and a Docker without user-id mapping (no rootless Docker, no `userns-remap`) | The [web UI](webui.md), its broker and the [chat](chat.md) only |

Recommended for optimal performance:

| Resource | Recommendation |
|---|---|
| Storage | Flash-based (NVMe SSDs strongly recommended) |
| CPU | High core count (8+) |
| Memory | 64GB+ RAM |
| GPU | NVIDIA GPU with 8GB+ VRAM (for the `ml` profile) |

ML classification can take days to months for large datasets. Check the benchmarks at [joaopn/encoder-optimization-guide](https://github.com/joaopn/encoder-optimization-guide) to estimate runtimes on your hardware.

## Install steps

**1. Clone the repository.**

```bash
git clone https://github.com/joaopn/social-data-pipeline
cd social-data-pipeline
```

**2. Pick how you run the CLI.** Both give the same commands; this guide writes `python sdp.py <command>` throughout.

- **Run directly** with `python sdp.py <command>`. Zero install. It requires `pyyaml` on the system Python (`pip install pyyaml`) or in an activated venv.
- **Install as a CLI** with `pipx install --editable .` (requires `pipx >= 1.4`). It gives a global `sdp` command, so `python sdp.py X` and `sdp X` are the same. The install is pinned to the clone path; reinstall if you move the clone.

```bash
pip install pyyaml                          # run directly: python sdp.py <command>
pipx install --editable .                   # or: the global `sdp`
```

**3. Check that it runs.**

```bash
python sdp.py --help                        # lists the command groups
```

**4. Go on to the [Quick start](quick-start.md).** It configures the databases, adds a source and runs the pipeline.

## Try it in your browser

The [Codespaces demo](https://codespaces.new/joaopn/social-data-pipeline?ref=main) runs the same pipeline in GitHub Codespaces, with zero install. Its setup lives in `.devcontainer/`:

| Piece | What it does |
|---|---|
| `.devcontainer/Dockerfile` | A `python:3.13-slim-bookworm` image with the Docker CLI, the Buildx and Compose plugins, git, pipx and PyYAML; it talks to the Codespace's Docker through the mounted Docker socket |
| `.devcontainer/post-create.sh` | Seeds the workspace from `.devcontainer/demo/` with pre-baked configuration (`.env`, PostgreSQL, MongoDB and StarRocks with their read-only MCP servers, the query scheduler, and a `reddit` source), pre-creates the data folders, and installs `sdp` with pipx; files that already exist are skipped |
| `.devcontainer/demo/WELCOME.md` | Opens when you attach: which database to start, where to put the dumps, and what to run |
| Forwarded ports | 8050 (query scheduler page), 8000 (PostgreSQL MCP), 3000 (MongoDB MCP), 9000 (StarRocks MCP), 5432 (PostgreSQL) |

In the Codespace you type `sdp <command>`, which is the same as `python sdp.py <command>`. Nothing runs at first: start one database (`sdp db start postgres` is the recommended default for the free tier) rather than all three, which can run the free-tier machine out of memory.

## Update

```bash
git pull                                    # picks up code changes
pipx reinstall social-data-pipeline         # pipx installs only, when a release changes pyproject.toml
python sdp.py run <profile> --build         # rebuild a stage's image after code or dependency changes
```

`git pull` is all a direct run needs. For `pipx` installs, also run `pipx reinstall social-data-pipeline` if a release changes `pyproject.toml` (rare; flagged in release notes). Pipeline images are rebuilt with `--build` (see [CLI reference](cli.md)); the database servers pick up config changes by a restart (`python sdp.py db stop`, then `python sdp.py db start`).
