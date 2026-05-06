"""Mount-coherence helpers for `db start` / `source add|remove` / `run`.

The pipeline lets the user mutate sources independently of database server
lifecycle:

    sdp db start postgres        # generates docker-compose.override.yml
                                 # with per-source mounts as they exist NOW
    sdp source add reddit        # writes config/sources/reddit/, but does
                                 # NOT touch override.yml or restart PG
    sdp run postgres_ingest -s reddit
                                 # ingest container expects PG to see
                                 # /data/parsed/reddit, but PG is still
                                 # running with the old mount set

Without a guard, the mismatch surfaces deep inside `pg_parquet`'s
COPY-from-file with an opaque "could not stat" error. This module is the
source of truth for "is the running DB container's mount set in sync with
the configured sources?".

Two surfaces consume it:

1. `cmd_source_add` / `cmd_source_remove` — after writing/deleting source
   config, compare the *file* (`docker-compose.override.yml`) against the
   new source set. If a running PG/SR container is now out of sync, warn
   the operator with the exact `db stop && db start` recovery line.

2. `cmd_run` (for `*_ingest` / `*_ml` profiles) — probe the running DB
   container with `docker inspect` and verify the per-source mounts the
   profile is about to need. Fail-fast with the same recovery line
   instead of letting the orchestrator container die deep in COPY.

The two callers use different *sources* (file vs. live container) on
purpose. The override file is the cheaper proxy at warning time
(operator just changed something locally, fastest signal); the inspect
probe is the authoritative check at run time (catches edge cases like a
manually edited override or a container started before the override was
last regenerated).
"""

from __future__ import annotations

# Per-server: which source-level profiles cause a source to need mounts on
# this server. Mirrors `_resolve_server_data_mounts` in sdp.py — kept in one
# place so adding a new ingest profile only needs editing this table.
SERVICE_PROFILES = {
    "postgres": frozenset({"postgres_ingest", "postgres_ml"}),
    "starrocks": frozenset({"sr_ingest", "sr_ml"}),
}

# Profile → DB service whose mount set the profile depends on at run time.
PROFILE_TO_SERVICE = {
    "postgres_ingest": "postgres",
    "postgres_ml": "postgres",
    "sr_ingest": "starrocks",
    "sr_ml": "starrocks",
}

# Substrings identifying mounts that come from `db setup` (tablespaces, SR
# storage, jobs export) rather than from per-source data. Drift detection
# ignores these — they're set-and-forget and not affected by `source add`.
_NON_SOURCE_MOUNT_MARKERS = (
    "/data/tablespace/",
    "/data/deploy/starrocks/",
    ":/jobs_export",
)

# Defaults for the parent mounts that the postgres + starrocks server blocks
# in docker-compose.yml bind in unconditionally (the ``${PARSED_PATH:-...}``
# and ``${OUTPUT_PATH:-...}`` lines). Sources whose host paths fall under
# these don't need per-source override entries — the parent mount already
# exposes them. Out-of-parent sources still need per-source mounts and are
# what the C6 source-add warning / drift detection covers.
PARSED_PATH_COMPOSE_DEFAULT = "./data/parsed"
OUTPUT_PATH_COMPOSE_DEFAULT = "./data/output"


def _normalize_path(path):
    """Strip ``./`` prefix and trailing slash for stable string comparison.

    Both inputs to a containment check are bind-mount paths read either from
    yaml / .env or from ``docker inspect``. Caller is responsible for
    resolving relative paths against the workspace root when needed; this
    helper only handles the cosmetic ``./`` and trailing-slash variants.
    """
    p = (path or "").rstrip("/")
    if p.startswith("./"):
        p = p[2:]
    return p


def is_path_under(host_path, parent_path):
    """True when ``host_path`` is the same as or a descendant of ``parent_path``.

    String-based check so the helpers in this module stay pure (no
    filesystem touch). Both sides are normalized via ``_normalize_path``,
    so ``./data/parsed`` and ``data/parsed`` compare equal. Empty inputs
    return False — neither side can claim coverage.
    """
    if not host_path or not parent_path:
        return False
    h = _normalize_path(host_path)
    p = _normalize_path(parent_path)
    if not h or not p:
        return False
    return h == p or h.startswith(p + "/")


def _mount_is_redundant_with_parents(mount_str, parent_paths):
    """True when a per-source mount string is already covered by a parent mount.

    ``mount_str`` looks like ``"<host>:/data/<parsed|output>/<source>:ro"``.
    Splits the host segment, classifies which parent (parsed vs output)
    applies based on the container destination, and defers to
    ``is_path_under``.
    """
    if not parent_paths:
        return False
    # Only a leading "host:dest..." form is relevant; anything else (e.g.
    # tablespace mounts already filtered upstream) is left alone.
    parts = mount_str.split(":")
    if len(parts) < 2:
        return False
    host_path, container_path = parts[0], parts[1]
    if container_path.startswith("/data/parsed/"):
        return is_path_under(host_path, parent_paths.get("parsed"))
    if container_path.startswith("/data/output/"):
        return is_path_under(host_path, parent_paths.get("output"))
    return False


def expected_source_mounts(sources_info, service, parent_paths=None):
    """Build the per-source mount set a service should have.

    Args:
        sources_info: list of dicts ``{name, profiles, paths}`` — the same
            shape returned by walking ``list_sources()`` +
            ``load_source_config()`` + ``get_source_profiles()``.
        service: ``"postgres"`` or ``"starrocks"``.
        parent_paths: optional ``{"parsed": <abs>, "output": <abs>}``. When
            given, sources whose paths fall under those parents are dropped
            (the docker-compose.yml parent bind already covers them; per-
            source override entries would be redundant).

    Returns:
        set[str] of mount strings in the form
        ``"<host_path>:/data/<parsed|output>/<source>:ro"``. Matches the
        format `_resolve_server_data_mounts` writes to the override.
    """
    needed = SERVICE_PROFILES.get(service, frozenset())
    out = set()
    for s in sources_info:
        if not (set(s.get("profiles", [])) & needed):
            continue
        paths = s.get("paths", {}) or {}
        for key, container_base in (("parsed", "/data/parsed"), ("output", "/data/output")):
            host_path = paths.get(key, "")
            if not host_path:
                continue
            if parent_paths and is_path_under(host_path, parent_paths.get(key)):
                # Already covered by the compose-file parent mount.
                continue
            out.add(f"{host_path}:{container_base}/{s['name']}:ro")
    return out


def parse_override_source_mounts(override_data, service, parent_paths=None):
    """Pull the per-source mounts out of a parsed override dict.

    Filters away tablespace / SR storage / jobs_export mounts so the result
    is comparable to ``expected_source_mounts``. When ``parent_paths`` is
    given, also strips entries that are now redundant with the compose-file
    parent mount — that way a freshly-regenerated override (which omits
    in-parent mounts) compares clean against drift detection.

    Args:
        override_data: parsed ``docker-compose.override.yml`` (or ``None``
            / ``{}`` if the file is missing).
        service: ``"postgres"`` or ``"starrocks"``.
        parent_paths: optional, see ``expected_source_mounts``.

    Returns:
        set[str] of mount strings.
    """
    services = (override_data or {}).get("services", {}) or {}
    svc = services.get(service) or {}
    out = set()
    for vol in svc.get("volumes", []) or []:
        v = str(vol)
        if any(marker in v for marker in _NON_SOURCE_MOUNT_MARKERS):
            continue
        if _mount_is_redundant_with_parents(v, parent_paths):
            continue
        out.add(v)
    return out


def compute_mount_drift(
    override_data, sources_info,
    services=("postgres", "starrocks"),
    parent_paths=None,
):
    """Compare expected per-source mounts to those in the override file.

    Args:
        override_data: parsed override.yml (dict) or None.
        sources_info: see ``expected_source_mounts``.
        services: which services to check (default both PG and SR).
        parent_paths: optional, see ``expected_source_mounts``. Threading
            this through keeps drift detection in lock-step with override
            regeneration: both sides ignore in-parent sources.

    Returns:
        dict mapping service name → ``{"missing": [...], "extra": [...]}``.
        Only services with drift appear; empty dict means coherent. Both
        lists are sorted for stable output.
    """
    drift = {}
    for svc in services:
        expected = expected_source_mounts(sources_info, svc, parent_paths=parent_paths)
        actual = parse_override_source_mounts(override_data, svc, parent_paths=parent_paths)
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        if missing or extra:
            drift[svc] = {"missing": missing, "extra": extra}
    return drift


def expected_runtime_mounts_for_source(source_name, source_paths):
    """Per-source mount destinations a running DB needs to see.

    Used by ``cmd_run`` to validate a live container's mount set. Returns a
    dict of ``destination → expected host source``, e.g.
    ``{"/data/parsed/reddit": "/abs/host/parsed/reddit"}``.

    Only ``parsed`` and ``output`` paths from the source config matter for
    server-side reads (see ``_pg_server_path`` / ``_sr_server_path`` in
    `db/{postgres,starrocks}/ingest.py`).
    """
    out = {}
    for key, container_base in (("parsed", "/data/parsed"), ("output", "/data/output")):
        host_path = (source_paths or {}).get(key, "")
        if host_path:
            out[f"{container_base}/{source_name}"] = host_path
    return out


def _mount_covers_target(mount_dest, mount_source, target_dest, target_host_path):
    """Does an inspect mount entry satisfy a (target_dest → target_host_path) read?

    Two cases:
      - Exact match: mount destination equals target destination AND mount
        source equals target host path.
      - Ancestor match: target destination is a strict descendant of mount
        destination, and the same relative offset applied to the mount's
        host source produces the target host path.

    The ancestor case is what lets the compose-file parent mount
    (e.g. ``./data/parsed → /data/parsed``) cover an in-parent source's
    expected destination (e.g. ``/data/parsed/reddit``) without a per-
    source override entry. For an out-of-parent source whose path lives on
    a different filesystem, the parent's host source plus the source-name
    offset doesn't match the source's actual host path — so the parent
    correctly does NOT cover it, and ``runtime_mount_drift`` flags the
    missing per-source mount.
    """
    if not mount_dest or not mount_source:
        return False
    md = _normalize_path(mount_dest)
    ms = _normalize_path(mount_source)
    td = _normalize_path(target_dest)
    th = _normalize_path(target_host_path)
    if not md or not ms or not td or not th:
        return False
    if md == td:
        return ms == th
    if td.startswith(md + "/"):
        offset = td[len(md):]  # leading "/<rest>"
        return _normalize_path(ms + offset) == th
    return False


def runtime_mount_drift(actual_mounts, source_name, source_paths):
    """Compare expected per-source destinations against a live container's mounts.

    Args:
        actual_mounts: list of dicts shaped like docker inspect's
            ``Mounts`` array (each item has ``Destination`` and
            ``Source`` keys at minimum).
        source_name: source being run.
        source_paths: ``paths`` block from the source's platform.yaml.
            Callers should resolve relative paths to absolute before
            calling — docker inspect's mount sources are always absolute,
            and the host-path comparison in ``_mount_covers_target`` is
            string-based.

    Returns:
        list of destination paths that are missing from the container.
        Empty list when the container is in sync. A parent mount that
        resolves to the source's host path (per ``_mount_covers_target``)
        is treated as covering the source — the dual-mount design means
        in-parent sources don't need per-source override entries.
    """
    expected = expected_runtime_mounts_for_source(source_name, source_paths)
    actuals = list(actual_mounts or [])
    missing = []
    for target_dest, target_host in expected.items():
        if not any(
            _mount_covers_target(
                m.get("Destination"), m.get("Source"),
                target_dest, target_host,
            )
            for m in actuals
        ):
            missing.append(target_dest)
    return sorted(missing)
