"""Drift detection across DB config / env / .ro_credentials / containers / mounts.

Backs `sdp db verify` (exits non-zero on drift, suitable for CI / scripted
preflight) and the drift section of `sdp db status` (always exits 0,
advisory). Both surfaces share the same findings model so the human-
readable messages stay consistent and a single test surface covers both.

What we check, per database:

1. Auth coherence: ``config/db/<db>.yaml auth: true`` ↔
   ``<DB>_AUTH_ENABLED=true`` ↔ ``.ro_credentials`` exists ↔ file mode
   0600 ↔ host-owned.
2. Container env coherence (when running): the container's
   ``<DB>_AUTH_ENABLED`` env matches ``.env``; the container is
   ``healthy`` when auth is on.
3. Mount coherence (PG, SR): ``docker-compose.override.yml`` carries the
   per-source mounts the configured sources need (delegates to
   ``setup.mount_sync.compute_mount_drift`` so the source-add warning
   and ``db verify`` agree on what counts as drift).

Cross-cutting checks (keyed under ``"mcp"`` and ``"jobs"``):

4. ``config/db/mcp.yaml`` enables MCPs only for DBs that exist and have
   ``.ro_credentials``.
5. ``config/jobs/config.local.yaml`` targets reference currently-
   configured DBs, and ``auth: true`` in jobs is paired with at least
   one DB that has auth enabled.

``compute_drift(ctx)`` is pure: the CLI wrapper (in ``sdp.py``) is
responsible for filesystem / docker probes and assembles ``ctx`` as a
plain dict. Tests construct ``ctx`` directly with mocked data, so the
verify logic is decoupled from the I/O layer.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


# Public categories. Keep this list in sync with the docstring above; the
# CLI uses these strings verbatim in --json output and human messages.
CATEGORIES = ("auth", "creds", "container", "mounts", "mcp", "jobs")


@dataclass(frozen=True)
class Finding:
    """One drift finding. ``category`` is a stable string from CATEGORIES;
    ``message`` describes the problem; ``fix`` names the recovery command."""
    category: str
    message: str
    fix: str

    def to_dict(self):
        return asdict(self)


# ---------------------------------------------------------------------------
# Per-DB checks.
# ---------------------------------------------------------------------------


def _auth_findings(db, db_yaml, env, cred_state):
    """Auth coherence between yaml, .env, and the on-disk RO credentials.

    cred_state is a dict the wrapper assembled. Required keys:
      - path: str ('' when no data path could be resolved)
      - exists: bool
      - mode: int | None  (filesystem mode bits, or None if exists is False)
      - host_owned: bool | None  (True only if uid matches the running user)
      - readable: bool  (False when the file exists but raised on read)
    """
    findings = []
    yaml_auth = bool(db_yaml.get("auth"))
    env_key = f"{db.upper()}_AUTH_ENABLED"
    env_auth = env.get(env_key) == "true"

    if yaml_auth and not env_auth:
        findings.append(Finding(
            category="auth",
            message=(
                f"config/db/{db}.yaml has auth:true but .env is missing "
                f"{env_key}=true; the DB entrypoint will not enforce auth"
            ),
            fix=f"sdp db setup --add {db}  # rewrites .env from yaml",
        ))
    elif env_auth and not yaml_auth:
        findings.append(Finding(
            category="auth",
            message=(
                f".env has {env_key}=true but config/db/{db}.yaml is "
                f"auth:false (or unset)"
            ),
            fix=f"sdp db setup --add {db}",
        ))

    auth_on = yaml_auth or env_auth
    if not auth_on:
        return findings

    path = cred_state.get("path") or f"<{db} data path>/.ro_credentials"
    if not cred_state.get("exists"):
        findings.append(Finding(
            category="creds",
            message=f"{db} auth enabled but {path} is missing",
            fix="sdp db recover-password --regenerate-ro",
        ))
        return findings

    if cred_state.get("host_owned") is False:
        findings.append(Finding(
            category="creds",
            message=(
                f"{path} is not host-owned (chowned to a container UID); "
                f"setup re-runs and verify cannot read it"
            ),
            fix=(
                f"sudo chown $(id -u):$(id -g) {path}  "
                f"# or: sdp db recover-password --regenerate-ro"
            ),
        ))

    mode = cred_state.get("mode")
    if mode is not None and (mode & 0o777) != 0o600:
        findings.append(Finding(
            category="creds",
            message=f"{path} mode is {oct(mode & 0o777)}, expected 0600",
            fix=f"chmod 600 {path}",
        ))

    if cred_state.get("readable") is False:
        findings.append(Finding(
            category="creds",
            message=f"{path} exists but cannot be read by the host user",
            fix=(
                f"sudo chown $(id -u):$(id -g) {path}  "
                f"# or: sdp db recover-password --regenerate-ro"
            ),
        ))

    return findings


def _container_findings(db, container_state, env):
    """Container-state coherence (only when probes were performed).

    container_state is None or a dict with optional keys:
      - running: bool
      - healthy: bool | None
      - env_auth: bool | None  (the value of <DB>_AUTH_ENABLED inside the container)

    A None container_state means "we did not probe" — this is the
    db-status path, where probes are skipped to keep status cheap.
    """
    if not container_state:
        return []
    if not container_state.get("running"):
        return []
    findings = []
    expected_auth = env.get(f"{db.upper()}_AUTH_ENABLED") == "true"
    actual = container_state.get("env_auth")
    if actual is not None and bool(actual) != bool(expected_auth):
        findings.append(Finding(
            category="container",
            message=(
                f"running {db} container has {db.upper()}_AUTH_ENABLED="
                f"{'true' if actual else 'false'} but .env now says "
                f"{'true' if expected_auth else 'false'}; container was "
                f"started before .env changed"
            ),
            fix=f"sdp db stop {db} && sdp db start {db}",
        ))
    if expected_auth and container_state.get("healthy") is False:
        findings.append(Finding(
            category="container",
            message=(
                f"{db} container is running but unhealthy; the auth-aware "
                f"healthcheck includes an RO-user probe, so this is "
                f"typically a missing or drifted RO password"
            ),
            fix=f"sdp db stop {db} && sdp db start {db}",
        ))
    return findings


def _mount_findings(db, override_data, sources_info, parent_paths=None):
    """Per-source mount coherence between override.yml and the source set.

    Mongo has no server-side mounts (mongoimport reads files in the
    ingest container), so this is a no-op for it. ``parent_paths`` (when
    given) tells the drift helper which sources are already covered by
    the docker-compose.yml parent mount — those are skipped on both sides
    of the comparison so they don't surface as drift.
    """
    if db not in ("postgres", "starrocks"):
        return []
    from social_data_pipeline.setup.mount_sync import compute_mount_drift
    drift = compute_mount_drift(
        override_data, sources_info,
        services=(db,), parent_paths=parent_paths,
    )
    findings = []
    d = drift.get(db)
    if not d:
        return findings
    for m in d.get("missing", []):
        findings.append(Finding(
            category="mounts",
            message=f"docker-compose.override.yml is missing source mount: {m}",
            fix=f"sdp db stop {db} && sdp db start {db}",
        ))
    for m in d.get("extra", []):
        findings.append(Finding(
            category="mounts",
            message=f"docker-compose.override.yml has stale source mount: {m}",
            fix=f"sdp db stop {db} && sdp db start {db}",
        ))
    return findings


# ---------------------------------------------------------------------------
# Cross-cutting checks (MCP, jobs).
# ---------------------------------------------------------------------------


def _mcp_findings(mcp_config, configured_dbs, cred_present):
    """MCP-config coherence: every enabled MCP needs its DB and creds."""
    if not mcp_config:
        return []
    findings = []
    for cfg_key in ("postgres", "mongo", "starrocks"):
        sub = mcp_config.get(cfg_key) or {}
        if not sub.get("enabled"):
            continue
        if cfg_key not in configured_dbs:
            findings.append(Finding(
                category="mcp",
                message=(
                    f"config/db/mcp.yaml enables {cfg_key}_mcp but "
                    f"{cfg_key} itself is not configured"
                ),
                fix=f"sdp db setup --add {cfg_key}  # or: sdp db unsetup-mcp",
            ))
            continue
        if not cred_present.get(cfg_key):
            findings.append(Finding(
                category="mcp",
                message=(
                    f"{cfg_key}_mcp is enabled but .ro_credentials is "
                    f"missing for {cfg_key}; the MCP entrypoint will fail loud"
                ),
                fix="sdp db recover-password --regenerate-ro",
            ))
    return findings


def _jobs_findings(jobs_config, configured_dbs, env):
    """Jobs scheduler coherence: targets reference real DBs; auth pairing."""
    if not jobs_config:
        return []
    findings = []
    backend_to_db = {
        "postgres": "postgres",
        "starrocks": "starrocks",
        "mongodb": "mongo",
    }
    for tname, spec in (jobs_config.get("targets") or {}).items():
        backend = (spec or {}).get("backend")
        db = backend_to_db.get(backend)
        if db is None:
            findings.append(Finding(
                category="jobs",
                message=(
                    f"jobs target {tname!r} has unknown backend "
                    f"{backend!r} (expected postgres / starrocks / mongodb)"
                ),
                fix="edit config/jobs/config.local.yaml or rerun 'sdp db setup-jobs'",
            ))
            continue
        if db not in configured_dbs:
            findings.append(Finding(
                category="jobs",
                message=(
                    f"jobs target {tname!r} routes to {backend} but "
                    f"{db} is not configured"
                ),
                fix=f"sdp db setup --add {db}  # or: sdp db unsetup-jobs",
            ))

    if jobs_config.get("auth"):
        any_db_auth = any(
            env.get(f"{db.upper()}_AUTH_ENABLED") == "true"
            for db in configured_dbs
        )
        if not any_db_auth:
            findings.append(Finding(
                category="jobs",
                message=(
                    "jobs config has auth:true but no configured DB has "
                    "auth enabled; the jobs UI password prompt will not "
                    "match any backend"
                ),
                fix="sdp db setup --add <db>  # turn auth on for at least one DB",
            ))
    return findings


# ---------------------------------------------------------------------------
# Top-level entry point.
# ---------------------------------------------------------------------------


def compute_drift(ctx):
    """Run every coherence check and bundle findings by name.

    ``ctx`` is a dict with the following keys (the CLI wrapper assembles
    it from filesystem / docker probes; tests pass it directly):

    - ``env``: ``dict[str, str]`` from ``.env``.
    - ``configured_dbs``: ``list[str]`` of db names with a yaml config.
    - ``db_yamls``: ``dict[db, dict]`` parsed from ``config/db/<db>.yaml``.
    - ``cred_file_states``: ``dict[db, dict]`` shape per ``_auth_findings``.
    - ``sources_info``: list of source dicts ``{name, profiles, paths}``.
    - ``override_data``: parsed ``docker-compose.override.yml`` (or ``{}``).
    - ``mcp_config``: parsed ``config/db/mcp.yaml`` (or ``{}``).
    - ``jobs_config``: merged jobs config (or ``{}``).
    - ``container_states``: ``dict[db, dict | None]``. ``None`` (or key
      absent) means "we didn't probe" — used by ``db status``, which
      stays local-files-only. ``db verify`` populates it.

    Returns a dict mapping name → list[Finding]. Names are db names plus
    the cross-cutting ``"mcp"`` and ``"jobs"`` keys. DB entries are
    always present for every configured DB (empty list = OK); cross-
    cutting entries appear only when there are findings to report.
    """
    out = {}
    env = ctx.get("env") or {}
    configured = list(ctx.get("configured_dbs") or [])
    cred_states = ctx.get("cred_file_states") or {}
    db_yamls = ctx.get("db_yamls") or {}
    sources_info = ctx.get("sources_info") or []
    override_data = ctx.get("override_data") or {}
    mcp_config = ctx.get("mcp_config") or {}
    jobs_config = ctx.get("jobs_config") or {}
    container_states = ctx.get("container_states") or {}
    parent_paths = ctx.get("parent_paths")

    for db in configured:
        findings = []
        findings.extend(_auth_findings(
            db, db_yamls.get(db, {}), env,
            cred_states.get(db) or {"exists": False, "path": ""},
        ))
        findings.extend(_container_findings(
            db, container_states.get(db), env,
        ))
        findings.extend(_mount_findings(
            db, override_data, sources_info, parent_paths=parent_paths,
        ))
        out[db] = findings

    cred_present = {
        db: bool((cred_states.get(db) or {}).get("exists"))
        for db in configured
    }
    mcp = _mcp_findings(mcp_config, configured, cred_present)
    if mcp:
        out["mcp"] = mcp
    jobs = _jobs_findings(jobs_config, configured, env)
    if jobs:
        out["jobs"] = jobs
    return out


def is_clean(findings_by_name):
    """True when every list in the findings dict is empty."""
    return all(not v for v in findings_by_name.values())
