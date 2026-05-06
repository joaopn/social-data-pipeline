"""Tests for mount-coherence helpers (Commit 6).

The helpers in `social_data_pipeline.setup.mount_sync` answer the
question "is the running DB container's mount set in sync with the
configured sources?". Two surfaces consume them:

1. `cmd_source_add` / `cmd_source_remove` — compares
   `docker-compose.override.yml` (the proxy for the running container's
   mounts after `db start`) against the configured sources, warning on
   drift.

2. `cmd_run` for `*_ingest` / `*_ml` — probes the live container's
   mount set with `docker inspect` and fails fast before launching the
   orchestrator, so the user sees the recovery hint at the CLI rather
   than an opaque pg_parquet error deep in COPY.

These tests cover both shapes: pure-data drift detection (override vs.
sources) and runtime drift detection (live mounts vs. expected per-source
destinations). They also pin the service-vs-profile mappings, since a
new ingest profile added to `VALID_PROFILES` without a matching entry
here would silently bypass mount validation — the same failure mode
`tests/setup/test_profile_gating.py` catches for the DB-config gate.
"""

from __future__ import annotations

import sdp
from social_data_pipeline.setup import mount_sync
from social_data_pipeline.setup.mount_sync import (
    SERVICE_PROFILES,
    PROFILE_TO_SERVICE,
    compute_mount_drift,
    expected_runtime_mounts_for_source,
    expected_source_mounts,
    is_path_under,
    parse_override_source_mounts,
    runtime_mount_drift,
)


# ---------------------------------------------------------------------------
# expected_source_mounts — does the right source-with-the-right-profile join.
# ---------------------------------------------------------------------------


class TestExpectedSourceMounts:
    def test_postgres_picks_up_postgres_ingest_source(self):
        sources = [{
            "name": "reddit",
            "profiles": ["parse", "postgres_ingest"],
            "paths": {
                "parsed": "/host/parsed/reddit",
                "output": "/host/output/reddit",
            },
        }]
        mounts = expected_source_mounts(sources, "postgres")
        assert mounts == {
            "/host/parsed/reddit:/data/parsed/reddit:ro",
            "/host/output/reddit:/data/output/reddit:ro",
        }

    def test_starrocks_picks_up_sr_ml_source(self):
        sources = [{
            "name": "twitter",
            "profiles": ["parse", "sr_ml"],
            "paths": {
                "parsed": "/host/parsed/twitter",
                "output": "/host/output/twitter",
            },
        }]
        # sr_ml alone qualifies a source for SR mounts (don't need sr_ingest).
        assert expected_source_mounts(sources, "starrocks") == {
            "/host/parsed/twitter:/data/parsed/twitter:ro",
            "/host/output/twitter:/data/output/twitter:ro",
        }

    def test_postgres_ignores_starrocks_only_source(self):
        # Source with only sr_ingest must not appear in postgres mounts.
        sources = [{
            "name": "twitter",
            "profiles": ["parse", "sr_ingest"],
            "paths": {"parsed": "/host/parsed/twitter", "output": "/host/output/twitter"},
        }]
        assert expected_source_mounts(sources, "postgres") == set()

    def test_starrocks_ignores_postgres_only_source(self):
        sources = [{
            "name": "reddit",
            "profiles": ["parse", "postgres_ingest"],
            "paths": {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        }]
        assert expected_source_mounts(sources, "starrocks") == set()

    def test_source_missing_paths_is_skipped(self):
        # No parsed/output → nothing to mount, even with a qualifying profile.
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {},
        }]
        assert expected_source_mounts(sources, "postgres") == set()

    def test_partial_paths_emit_only_present_keys(self):
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {"parsed": "/host/parsed/reddit"},  # no output
        }]
        assert expected_source_mounts(sources, "postgres") == {
            "/host/parsed/reddit:/data/parsed/reddit:ro",
        }

    def test_multiple_sources_aggregate(self):
        sources = [
            {
                "name": "reddit",
                "profiles": ["postgres_ingest"],
                "paths": {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
            },
            {
                "name": "twitter",
                "profiles": ["postgres_ml"],
                "paths": {"parsed": "/host/parsed/twitter", "output": "/host/output/twitter"},
            },
        ]
        mounts = expected_source_mounts(sources, "postgres")
        # Both sources qualify (postgres_ingest and postgres_ml are both in the
        # postgres bundle); each contributes parsed + output.
        assert len(mounts) == 4
        assert "/host/parsed/reddit:/data/parsed/reddit:ro" in mounts
        assert "/host/parsed/twitter:/data/parsed/twitter:ro" in mounts


# ---------------------------------------------------------------------------
# parse_override_source_mounts — strips setup-generated mounts (tablespaces,
# SR storage, jobs export) so the result is comparable to expected.
# ---------------------------------------------------------------------------


class TestParseOverrideSourceMounts:
    def test_returns_per_source_mounts(self):
        override = {
            "services": {
                "postgres": {
                    "volumes": [
                        "/host/parsed/reddit:/data/parsed/reddit:ro",
                        "/host/output/reddit:/data/output/reddit:ro",
                    ],
                },
            },
        }
        assert parse_override_source_mounts(override, "postgres") == {
            "/host/parsed/reddit:/data/parsed/reddit:ro",
            "/host/output/reddit:/data/output/reddit:ro",
        }

    def test_strips_tablespace_mounts(self):
        override = {
            "services": {
                "postgres": {
                    "volumes": [
                        "/host/ts/fast:/data/tablespace/fast:rw",
                        "/host/parsed/reddit:/data/parsed/reddit:ro",
                    ],
                },
            },
        }
        assert parse_override_source_mounts(override, "postgres") == {
            "/host/parsed/reddit:/data/parsed/reddit:ro",
        }

    def test_strips_starrocks_storage_mount(self):
        override = {
            "services": {
                "starrocks": {
                    "volumes": [
                        "/host/sr-be:/data/deploy/starrocks/be:rw",
                        "/host/parsed/twitter:/data/parsed/twitter:ro",
                    ],
                },
            },
        }
        assert parse_override_source_mounts(override, "starrocks") == {
            "/host/parsed/twitter:/data/parsed/twitter:ro",
        }

    def test_strips_jobs_export_mount(self):
        override = {
            "services": {
                "postgres": {
                    "volumes": [
                        "./data/jobs/results:/jobs_export:rw",
                        "/host/parsed/reddit:/data/parsed/reddit:ro",
                    ],
                },
            },
        }
        assert parse_override_source_mounts(override, "postgres") == {
            "/host/parsed/reddit:/data/parsed/reddit:ro",
        }

    def test_handles_missing_service(self):
        override = {"services": {"postgres": {"volumes": []}}}
        assert parse_override_source_mounts(override, "starrocks") == set()

    def test_handles_empty_input(self):
        assert parse_override_source_mounts({}, "postgres") == set()
        assert parse_override_source_mounts(None, "postgres") == set()


# ---------------------------------------------------------------------------
# compute_mount_drift — the helper exercised by `cmd_source_add/remove`.
# ---------------------------------------------------------------------------


class TestComputeMountDrift:
    def _override_with(self, postgres_vols=None, starrocks_vols=None):
        return {
            "services": {
                "postgres": {"volumes": list(postgres_vols or [])},
                "starrocks": {"volumes": list(starrocks_vols or [])},
            },
        }

    def test_no_drift_when_override_matches_sources(self):
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        }]
        override = self._override_with(postgres_vols=[
            "/host/parsed/reddit:/data/parsed/reddit:ro",
            "/host/output/reddit:/data/output/reddit:ro",
        ])
        assert compute_mount_drift(override, sources) == {}

    def test_no_drift_with_no_sources_and_empty_override(self):
        # Plain `db setup` (no sources yet) is the steady state for a fresh
        # install — must not register as drift.
        assert compute_mount_drift({}, []) == {}

    def test_missing_source_reported_as_missing(self):
        # Sources include reddit, but override has no per-source mounts.
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        }]
        override = self._override_with()
        drift = compute_mount_drift(override, sources)
        assert "postgres" in drift
        assert drift["postgres"]["missing"] == [
            "/host/output/reddit:/data/output/reddit:ro",
            "/host/parsed/reddit:/data/parsed/reddit:ro",
        ]
        assert drift["postgres"]["extra"] == []
        assert "starrocks" not in drift  # no SR sources, override has nothing → coherent

    def test_removed_source_reported_as_extra(self):
        # Override still mounts a source the user just removed.
        override = self._override_with(postgres_vols=[
            "/host/parsed/reddit:/data/parsed/reddit:ro",
            "/host/output/reddit:/data/output/reddit:ro",
        ])
        drift = compute_mount_drift(override, [])
        assert drift["postgres"]["missing"] == []
        assert drift["postgres"]["extra"] == [
            "/host/output/reddit:/data/output/reddit:ro",
            "/host/parsed/reddit:/data/parsed/reddit:ro",
        ]

    def test_distinguishes_pg_from_sr(self):
        # Two sources: one PG-only, one SR-only. Override only has PG mounts.
        # Drift report must put PG in coherent state and SR in missing.
        sources = [
            {
                "name": "reddit",
                "profiles": ["postgres_ingest"],
                "paths": {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
            },
            {
                "name": "twitter",
                "profiles": ["sr_ingest"],
                "paths": {"parsed": "/host/parsed/twitter", "output": "/host/output/twitter"},
            },
        ]
        override = self._override_with(postgres_vols=[
            "/host/parsed/reddit:/data/parsed/reddit:ro",
            "/host/output/reddit:/data/output/reddit:ro",
        ])
        drift = compute_mount_drift(override, sources)
        assert "postgres" not in drift  # matches
        assert drift["starrocks"]["missing"] == [
            "/host/output/twitter:/data/output/twitter:ro",
            "/host/parsed/twitter:/data/parsed/twitter:ro",
        ]

    def test_setup_only_mounts_do_not_count_as_extra(self):
        # Override has tablespace + jobs_export but no source mounts; with no
        # sources configured this is steady-state.
        override = self._override_with(postgres_vols=[
            "/host/ts/fast:/data/tablespace/fast:rw",
            "./data/jobs/results:/jobs_export:rw",
        ])
        assert compute_mount_drift(override, []) == {}

    def test_services_filter_is_honored(self):
        # When only PG is being checked (e.g. only postgres is running), SR
        # drift must not be reported even if the source set has SR sources.
        sources = [{
            "name": "twitter",
            "profiles": ["sr_ingest"],
            "paths": {"parsed": "/host/parsed/twitter", "output": "/host/output/twitter"},
        }]
        drift = compute_mount_drift({}, sources, services=("postgres",))
        assert drift == {}


# ---------------------------------------------------------------------------
# Runtime drift — what `cmd_run` uses to validate a live container.
# ---------------------------------------------------------------------------


class TestExpectedRuntimeMounts:
    def test_returns_destination_keyed_dict(self):
        out = expected_runtime_mounts_for_source(
            "reddit",
            {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        )
        assert out == {
            "/data/parsed/reddit": "/host/parsed/reddit",
            "/data/output/reddit": "/host/output/reddit",
        }

    def test_skips_missing_paths(self):
        # Source has no `output` key — should still produce the parsed entry.
        out = expected_runtime_mounts_for_source(
            "reddit", {"parsed": "/host/parsed/reddit"},
        )
        assert out == {"/data/parsed/reddit": "/host/parsed/reddit"}

    def test_empty_paths_returns_empty(self):
        assert expected_runtime_mounts_for_source("reddit", {}) == {}
        assert expected_runtime_mounts_for_source("reddit", None) == {}


class TestRuntimeMountDrift:
    def test_in_sync_returns_empty(self):
        # Container has both expected destinations.
        actual = [
            {"Destination": "/data/parsed/reddit", "Source": "/host/parsed/reddit"},
            {"Destination": "/data/output/reddit", "Source": "/host/output/reddit"},
        ]
        missing = runtime_mount_drift(
            actual, "reddit",
            {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        )
        assert missing == []

    def test_missing_dest_is_reported(self):
        # Container missing /data/output/reddit (e.g. source added after start).
        actual = [
            {"Destination": "/data/parsed/reddit", "Source": "/host/parsed/reddit"},
        ]
        missing = runtime_mount_drift(
            actual, "reddit",
            {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        )
        assert missing == ["/data/output/reddit"]

    def test_completely_missing_source_reports_all(self):
        # Container has no per-source mounts at all (started before source add).
        actual = [
            {"Destination": "/data/tablespace/fast", "Source": "/host/ts/fast"},
        ]
        missing = runtime_mount_drift(
            actual, "reddit",
            {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        )
        assert missing == ["/data/output/reddit", "/data/parsed/reddit"]

    def test_handles_missing_actual(self):
        assert runtime_mount_drift(None, "reddit", {"parsed": "/x"}) == ["/data/parsed/reddit"]
        assert runtime_mount_drift([], "reddit", {}) == []


# ---------------------------------------------------------------------------
# Lookup-table coverage — pin every ingest profile to a service so a future
# `*_ingest` / `*_ml` profile can't slip past mount validation in cmd_run.
# ---------------------------------------------------------------------------


class TestProfileServiceCoverage:
    def test_every_pg_sr_profile_is_in_profile_to_service(self):
        # Every profile that ingests into PG or SR must declare its service so
        # cmd_run knows which container to inspect.
        ingest_profiles = {"postgres_ingest", "postgres_ml", "sr_ingest", "sr_ml"}
        assert ingest_profiles == set(PROFILE_TO_SERVICE)

    def test_profile_to_service_values_match_service_profiles(self):
        # Inverse mapping: every PROFILE_TO_SERVICE entry must show up in the
        # corresponding SERVICE_PROFILES bundle. Catches drift between the two
        # tables (which would silently misroute mounts).
        for profile, service in PROFILE_TO_SERVICE.items():
            assert profile in SERVICE_PROFILES[service], (
                f"{profile} → {service} declared, but {profile} not in "
                f"SERVICE_PROFILES[{service!r}]"
            )

    def test_mongo_ingest_not_in_profile_to_service(self):
        # mongo_ingest reads files in the *ingest container* (mongoimport),
        # not server-side. It must NOT trigger PG/SR-style mount validation.
        assert "mongo_ingest" not in PROFILE_TO_SERVICE


# ---------------------------------------------------------------------------
# cmd_run wiring — drifted mount set surfaces as exit 1 with hint.
# ---------------------------------------------------------------------------


class TestCmdRunMountValidation:
    def test_exits_1_when_running_container_missing_source_mount(self, tmp_path, monkeypatch, capsys):
        """Live container running, but missing /data/parsed/reddit → exit 1.

        End-to-end through `_validate_run_mounts` so the wiring (running
        services check → container inspect → drift compute → human-readable
        error) is exercised together.
        """
        # postgres is "running"
        monkeypatch.setattr(sdp, "_running_services", lambda: {"postgres"})
        # ...but its mount set has no per-source mounts
        monkeypatch.setattr(
            sdp, "_container_mounts",
            lambda service: [
                {"Destination": "/data/tablespace/fast", "Source": "/host/ts/fast"},
            ],
        )

        rc = sdp._validate_run_mounts(
            "postgres_ingest", "reddit",
            {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        )

        out = capsys.readouterr().out
        assert rc == 1
        assert "missing mount(s)" in out
        assert "/data/parsed/reddit" in out
        assert "/data/output/reddit" in out
        # Recovery hint must name the exact two-step.
        assert "db stop postgres" in out and "db start postgres" in out

    def test_returns_0_when_service_not_running(self, monkeypatch):
        # cmd_run can be invoked before db start; compose run will start
        # the container fresh with the current override. No validation needed.
        monkeypatch.setattr(sdp, "_running_services", lambda: set())
        # _container_mounts must NOT be called in this branch.
        monkeypatch.setattr(
            sdp, "_container_mounts",
            lambda service: (_ for _ in ()).throw(AssertionError("should not be called")),
        )

        rc = sdp._validate_run_mounts(
            "postgres_ingest", "reddit",
            {"parsed": "/host/parsed/reddit"},
        )
        assert rc == 0

    def test_returns_0_when_mounts_match(self, monkeypatch):
        monkeypatch.setattr(sdp, "_running_services", lambda: {"postgres"})
        monkeypatch.setattr(
            sdp, "_container_mounts",
            lambda service: [
                {"Destination": "/data/parsed/reddit", "Source": "/host/parsed/reddit"},
                {"Destination": "/data/output/reddit", "Source": "/host/output/reddit"},
            ],
        )
        rc = sdp._validate_run_mounts(
            "postgres_ingest", "reddit",
            {"parsed": "/host/parsed/reddit", "output": "/host/output/reddit"},
        )
        assert rc == 0

    def test_non_ingest_profile_skips_validation(self, monkeypatch):
        # `parse` doesn't depend on a DB server; PROFILE_TO_SERVICE has no
        # entry, so validation is a no-op even with a stale running PG.
        monkeypatch.setattr(
            sdp, "_running_services",
            lambda: (_ for _ in ()).throw(AssertionError("should not be called")),
        )
        rc = sdp._validate_run_mounts(
            "parse", "reddit",
            {"parsed": "/host/parsed/reddit"},
        )
        assert rc == 0

    def test_sr_profile_targets_starrocks_container(self, monkeypatch):
        # sr_ingest must inspect the starrocks container, not postgres.
        seen_services = []

        def fake_running():
            return {"starrocks"}

        def fake_inspect(service):
            seen_services.append(service)
            return [{"Destination": "/data/parsed/reddit", "Source": "/host/parsed/reddit"}]

        monkeypatch.setattr(sdp, "_running_services", fake_running)
        monkeypatch.setattr(sdp, "_container_mounts", fake_inspect)

        rc = sdp._validate_run_mounts(
            "sr_ingest", "reddit", {"parsed": "/host/parsed/reddit"},
        )
        assert rc == 0
        assert seen_services == ["starrocks"]

    def test_symlinked_workspace_canonicalized_through_realpath(self, tmp_path, monkeypatch):
        """ROOT and docker-inspect Sources canonicalize via realpath.

        The bug: a symlinked workspace (``/home/user/repo`` → ``/data/.../repo``)
        produces a string mismatch — ROOT resolves to one form,
        ``docker inspect`` reports the other for the parent mount Source.
        Without canonicalization, the parent-mount ancestor check rejects
        the offset and the validator false-positives a missing mount.

        This test stages a real symlink in tmp_path: ``link/data/parsed``
        points at ``real/data/parsed``. ROOT is set to ``link/`` (the
        ``__file__`` form), and the inspect Source is reported as the
        ``real/`` form (what docker stores). Realpath on both sides should
        canonicalize them and the validator should accept the parent mount.
        """
        real = tmp_path / "real"
        link = tmp_path / "link"
        (real / "data" / "parsed" / "reddit").mkdir(parents=True)
        link.symlink_to(real)

        monkeypatch.setattr(sdp, "ROOT", link)
        monkeypatch.setattr(sdp, "_running_services", lambda: {"postgres"})
        monkeypatch.setattr(
            sdp, "_container_mounts",
            lambda service: [
                # docker inspect reports the resolved-real path here even
                # though compose was invoked from the symlinked workspace.
                {"Destination": "/data/parsed", "Source": str(real / "data" / "parsed")},
            ],
        )

        # Source path is relative — gets resolved against ROOT (the symlink)
        # via Path operations; realpath then canonicalizes through the
        # symlink to match the inspect Source.
        rc = sdp._validate_run_mounts(
            "postgres_ingest", "reddit",
            {"parsed": "./data/parsed/reddit"},
        )
        assert rc == 0


# ---------------------------------------------------------------------------
# Parent-mount coverage — the dual-mount design where docker-compose.yml
# binds ${PARSED_PATH} / ${OUTPUT_PATH} on the DB server. In-parent sources
# don't need per-source override entries; out-of-parent sources still do.
# ---------------------------------------------------------------------------


class TestIsPathUnder:
    def test_relative_default_path_match(self):
        # The compose-file default ./data/parsed contains ./data/parsed/reddit.
        # './' prefix on either side normalizes away.
        assert is_path_under("./data/parsed/reddit", "./data/parsed") is True
        assert is_path_under("data/parsed/reddit", "./data/parsed") is True
        assert is_path_under("./data/parsed/reddit", "data/parsed") is True

    def test_absolute_path_match(self):
        assert is_path_under("/mnt/ssd/parsed/twitter", "/mnt/ssd/parsed") is True
        # Trailing slash on parent doesn't break it.
        assert is_path_under("/mnt/ssd/parsed/twitter", "/mnt/ssd/parsed/") is True

    def test_equality_counts_as_under(self):
        # The parent itself is "under" (== same path) — degenerate but
        # well-defined; matters when a source's path is exactly the parent.
        assert is_path_under("./data/parsed", "./data/parsed") is True

    def test_different_filesystems_not_under(self):
        # Default parent doesn't cover a multi-disk source.
        assert is_path_under("/mnt/ssd/parsed/twitter", "./data/parsed") is False
        assert is_path_under("./data/parsed/reddit", "/mnt/ssd/parsed") is False

    def test_partial_prefix_not_under(self):
        # `./data/parsed_v2` must not be treated as under `./data/parsed` —
        # the helper requires a `/` boundary between parent and child.
        assert is_path_under("./data/parsed_v2/reddit", "./data/parsed") is False

    def test_empty_inputs_return_false(self):
        # Defensive — a missing parent_paths entry shouldn't accidentally
        # claim every source is covered.
        assert is_path_under("", "./data/parsed") is False
        assert is_path_under("./data/parsed/reddit", "") is False
        assert is_path_under("", "") is False


class TestExpectedSourceMountsWithParents:
    def test_in_parent_source_skipped(self):
        # Default-path source (parsed under ./data/parsed): the parent mount
        # in docker-compose.yml already exposes it, so per-source override
        # entry would be redundant. Filter drops it.
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "./data/parsed/reddit",
                "output": "./data/output/reddit",
            },
        }]
        out = expected_source_mounts(
            sources, "postgres",
            parent_paths={"parsed": "./data/parsed", "output": "./data/output"},
        )
        assert out == set()

    def test_out_of_parent_source_retained(self):
        # Multi-disk source: paths live outside ${PARSED_PATH}/${OUTPUT_PATH}.
        # Filter keeps them — they need explicit override entries.
        sources = [{
            "name": "twitter",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "/mnt/ssd/parsed/twitter",
                "output": "/mnt/ssd/output/twitter",
            },
        }]
        out = expected_source_mounts(
            sources, "postgres",
            parent_paths={"parsed": "./data/parsed", "output": "./data/output"},
        )
        assert out == {
            "/mnt/ssd/parsed/twitter:/data/parsed/twitter:ro",
            "/mnt/ssd/output/twitter:/data/output/twitter:ro",
        }

    def test_mixed_in_out_parent(self):
        # parsed in-parent, output out-of-parent → only the output mount.
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "./data/parsed/reddit",        # under parent
                "output": "/mnt/big/output/reddit",      # outside parent
            },
        }]
        out = expected_source_mounts(
            sources, "postgres",
            parent_paths={"parsed": "./data/parsed", "output": "./data/output"},
        )
        assert out == {"/mnt/big/output/reddit:/data/output/reddit:ro"}

    def test_no_parent_paths_legacy_behavior(self):
        # parent_paths=None preserves the C6 behavior — emit every per-source
        # mount unconditionally. Lets older callers (and the legacy override
        # regen path) still get full mount sets.
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "./data/parsed/reddit",
                "output": "./data/output/reddit",
            },
        }]
        out = expected_source_mounts(sources, "postgres", parent_paths=None)
        assert out == {
            "./data/parsed/reddit:/data/parsed/reddit:ro",
            "./data/output/reddit:/data/output/reddit:ro",
        }


class TestParseOverrideWithParents:
    def test_in_parent_override_entries_filtered(self):
        # An override file from a previous (pre-fix) regen still has per-
        # source entries for in-parent sources. Drift detection treats them
        # as redundant rather than "extra," because the new regen will drop
        # them on next db start.
        override = {
            "services": {
                "postgres": {
                    "volumes": [
                        "./data/parsed/reddit:/data/parsed/reddit:ro",
                        "./data/output/reddit:/data/output/reddit:ro",
                    ],
                },
            },
        }
        out = parse_override_source_mounts(
            override, "postgres",
            parent_paths={"parsed": "./data/parsed", "output": "./data/output"},
        )
        assert out == set()

    def test_out_of_parent_override_entries_retained(self):
        override = {
            "services": {
                "postgres": {
                    "volumes": [
                        "/mnt/ssd/parsed/twitter:/data/parsed/twitter:ro",
                    ],
                },
            },
        }
        out = parse_override_source_mounts(
            override, "postgres",
            parent_paths={"parsed": "./data/parsed", "output": "./data/output"},
        )
        assert out == {"/mnt/ssd/parsed/twitter:/data/parsed/twitter:ro"}


class TestComputeMountDriftWithParents:
    def test_in_parent_source_no_drift_even_without_override(self):
        # The bug we're closing: with the C6-only behavior, this combination
        # would produce a "missing source mount" finding. With parent_paths,
        # the parent mount in docker-compose.yml covers it and drift is empty.
        sources = [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "./data/parsed/reddit",
                "output": "./data/output/reddit",
            },
        }]
        drift = compute_mount_drift(
            override_data={},  # nothing in override
            sources_info=sources,
            parent_paths={"parsed": "./data/parsed", "output": "./data/output"},
        )
        assert drift == {}

    def test_out_of_parent_source_still_drifts_when_override_empty(self):
        # The case we still want to flag: a source on a different filesystem
        # NEEDS an override entry — parent mount can't cover it.
        sources = [{
            "name": "twitter",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "/mnt/ssd/parsed/twitter",
                "output": "/mnt/ssd/output/twitter",
            },
        }]
        drift = compute_mount_drift(
            override_data={},
            sources_info=sources,
            parent_paths={"parsed": "./data/parsed", "output": "./data/output"},
        )
        assert "postgres" in drift
        assert drift["postgres"]["missing"] == [
            "/mnt/ssd/output/twitter:/data/output/twitter:ro",
            "/mnt/ssd/parsed/twitter:/data/parsed/twitter:ro",
        ]


class TestRuntimeMountDriftWithParent:
    def test_parent_mount_covers_in_parent_source(self):
        # Container has only the parent mount (default-disk source set);
        # reading /data/parsed/reddit resolves to /abs/data/parsed/reddit
        # on host, which equals the source's host path. No drift.
        actual = [
            {"Destination": "/data/parsed", "Source": "/abs/data/parsed"},
            {"Destination": "/data/output", "Source": "/abs/data/output"},
        ]
        missing = runtime_mount_drift(
            actual, "reddit",
            {
                "parsed": "/abs/data/parsed/reddit",
                "output": "/abs/data/output/reddit",
            },
        )
        assert missing == []

    def test_parent_mount_does_not_cover_out_of_parent_source(self):
        # Parent maps /abs/data/parsed → /data/parsed. A source whose host
        # path is /mnt/ssd/parsed/twitter cannot be reached via the parent —
        # the offset doesn't produce the right host path. Per-source mount
        # is required, and absence is drift.
        actual = [
            {"Destination": "/data/parsed", "Source": "/abs/data/parsed"},
        ]
        missing = runtime_mount_drift(
            actual, "twitter",
            {"parsed": "/mnt/ssd/parsed/twitter"},
        )
        assert missing == ["/data/parsed/twitter"]

    def test_explicit_per_source_mount_still_works(self):
        # Out-of-parent source covered by an explicit override entry —
        # the same code path that pre-fix runtime_mount_drift handled.
        actual = [
            {"Destination": "/data/parsed/twitter", "Source": "/mnt/ssd/parsed/twitter"},
        ]
        missing = runtime_mount_drift(
            actual, "twitter",
            {"parsed": "/mnt/ssd/parsed/twitter"},
        )
        assert missing == []

    def test_parent_mount_with_wrong_host_source_does_not_cover(self):
        # Parent destination is right but the host source maps to a different
        # filesystem — the offset rule rejects this. Defensive: catches the
        # case where the user's PARSED_PATH was changed and DB hasn't been
        # restarted (the running container has stale parent host source).
        actual = [
            {"Destination": "/data/parsed", "Source": "/old/path/parsed"},
        ]
        missing = runtime_mount_drift(
            actual, "reddit",
            {"parsed": "/abs/data/parsed/reddit"},
        )
        # /old/path/parsed + /reddit = /old/path/parsed/reddit ≠ /abs/data/parsed/reddit
        assert missing == ["/data/parsed/reddit"]


# ---------------------------------------------------------------------------
# CLI wiring — _resolve_server_data_mounts skips in-parent sources.
# ---------------------------------------------------------------------------


class TestResolveServerDataMountsParentAware:
    def test_in_parent_source_only_writes_no_data_block(self, tmp_path, monkeypatch):
        """One in-parent source + nothing preserved → override file stripped.

        Confirms the override regen no longer writes redundant per-source
        mounts when the source's paths fall under ${PARSED_PATH}.
        """
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        # Empty .env → parent paths default to ./data/parsed and ./data/output.
        monkeypatch.setattr(sdp, "load_env", lambda: {})
        monkeypatch.setattr(sdp, "_collect_source_info", lambda: [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "./data/parsed/reddit",
                "output": "./data/output/reddit",
            },
        }])

        sdp._resolve_server_data_mounts(["postgres"])

        override_path = tmp_path / "docker-compose.override.yml"
        # No preserved entries, all sources in-parent → file shouldn't exist.
        assert not override_path.exists()

    def test_out_of_parent_source_still_writes_per_source_mount(self, tmp_path, monkeypatch):
        """Multi-disk source needs an explicit override entry."""
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "load_env", lambda: {})
        monkeypatch.setattr(sdp, "_collect_source_info", lambda: [{
            "name": "twitter",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "/mnt/ssd/parsed/twitter",
                "output": "/mnt/ssd/output/twitter",
            },
        }])

        sdp._resolve_server_data_mounts(["postgres"])

        override_path = tmp_path / "docker-compose.override.yml"
        assert override_path.exists()
        content = override_path.read_text()
        assert "/mnt/ssd/parsed/twitter:/data/parsed/twitter:ro" in content
        assert "/mnt/ssd/output/twitter:/data/output/twitter:ro" in content

    def test_preserved_entries_kept_even_when_no_per_source_data(self, tmp_path, monkeypatch):
        """A preexisting tablespace mount survives even with all-in-parent sources."""
        override_path = tmp_path / "docker-compose.override.yml"
        override_path.write_text(
            "services:\n"
            "  postgres:\n"
            "    volumes:\n"
            "      - /host/ts/fast:/data/tablespace/fast\n"
        )
        monkeypatch.setattr(sdp, "ROOT", tmp_path)
        monkeypatch.setattr(sdp, "load_env", lambda: {})
        monkeypatch.setattr(sdp, "_collect_source_info", lambda: [{
            "name": "reddit",
            "profiles": ["postgres_ingest"],
            "paths": {
                "parsed": "./data/parsed/reddit",
                "output": "./data/output/reddit",
            },
        }])

        sdp._resolve_server_data_mounts(["postgres"])

        # Tablespace mount must survive — unrelated to source mount filtering.
        content = override_path.read_text()
        assert "/host/ts/fast:/data/tablespace/fast" in content
        # And no redundant per-source reddit mount got written back.
        assert "/data/parsed/reddit" not in content


class TestGetParentPaths:
    def test_uses_compose_defaults_when_env_unset(self, monkeypatch):
        # No PARSED_PATH/OUTPUT_PATH in .env → falls back to the same
        # defaults docker-compose.yml uses.
        monkeypatch.setattr(sdp, "load_env", lambda: {})
        out = sdp._get_parent_paths()
        assert out == {"parsed": "./data/parsed", "output": "./data/output"}

    def test_reads_env_overrides(self, monkeypatch):
        env = {"PARSED_PATH": "/mnt/ssd/parsed", "OUTPUT_PATH": "/mnt/ssd/output"}
        out = sdp._get_parent_paths(env)
        assert out == {"parsed": "/mnt/ssd/parsed", "output": "/mnt/ssd/output"}

    def test_partial_override(self, monkeypatch):
        # Only PARSED_PATH overridden — OUTPUT_PATH still defaults.
        out = sdp._get_parent_paths({"PARSED_PATH": "/mnt/ssd/parsed"})
        assert out == {"parsed": "/mnt/ssd/parsed", "output": "./data/output"}
