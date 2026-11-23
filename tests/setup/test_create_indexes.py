"""Tests for `do_db_create_indexes` and `get_db_index_targets`, the index build and the column listing without a terminal.

The contracts pinned here:

- For the same answers, the action runs exactly the statements `sdp db create-indexes` runs, in the same order, and saves the same platform.yaml: the terminal is driven through its questions, the action with the equivalent request, both against the same stand-in databases behind `docker compose exec`.
- A table or column that the database does not have now is refused by the action itself, before any statement runs: the web form's picker is not the check.
- The listing is bounded and offers only names a request can carry.

The stand-in answers the `docker compose exec` command lines the CLI builds (psql, mongosh, mysql) from a small in-memory state and records every statement that changes something.
"""

from __future__ import annotations

import argparse
import json
import subprocess

import pytest
import yaml

import sdp
from social_data_pipeline.setup import utils

PLATFORM = {
    "db_schema": "reddit",
    "data_types": ["submissions", "comments"],
    "mandatory_fields": ["dataset", "id"],
    "fields": {"comments": ["author", "body", "subreddit"], "submissions": ["author", "title"]},
    "mongo_db_name_template": "{platform}_{data_type}",
}


class FakeDatabases:
    """The three databases as `docker compose exec` reaches them. `tables` maps a table to its (column, type) pairs; `executed` lists (database, statement) for every statement that changes something, in order; `seen` every command line."""

    def __init__(self, tables=None, collections=None):
        self.tables = tables if tables is not None else {
            "comments": [("dataset", "varchar"), ("id", "varchar"), ("author", "varchar"), ("score", "int"), ("controversiality", "tinyint"), ("upvote_ratio", "double")],
            "submissions": [("dataset", "varchar"), ("id", "varchar"), ("author", "varchar"), ("title", "varchar")],
            "comments_lingua": [("id", "varchar"), ("lang", "varchar")],
        }
        self.collections = collections if collections is not None else {"reddit_comments": ["RC_2024-02", "RC_2024-01"], "reddit_submissions": ["RS_2024-01"]}
        self.bloom = {"comments": {"dataset"}}
        self.executed = []
        self.seen = []

    def _ok(self, argv, stdout=""):
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    def run(self, argv, **kwargs):
        argv = list(argv)
        self.seen.append(argv)
        assert argv[:4] == ["docker", "compose", "exec", "-T"], f"unexpected subprocess: {argv}"
        service = argv[4]
        if service == "postgres":
            return self._postgres(argv)
        if service == "starrocks":
            return self._starrocks(argv)
        if service == "mongo":
            return self._mongo(argv)
        raise AssertionError(f"unexpected service: {argv}")

    def _rows(self, rows):
        return "".join("\t".join(row) + "\n" for row in rows)

    def _postgres(self, argv):
        statement = argv[argv.index("-c") + 1]
        if "-t" not in argv:
            self.executed.append(("postgres", statement))
            return self._ok(argv)
        if "FROM information_schema.tables" in statement:
            return self._ok(argv, self._rows([[t] for t in sorted(self.tables)]))
        if "FROM information_schema.columns" in statement:
            # The terminal asks per table, the listing for the whole schema.
            if "table_name = '" in statement:
                table = statement.split("table_name = '")[1].split("'")[0]
                return self._ok(argv, self._rows([[c] for c, _ in self.tables.get(table, [])]))
            return self._ok(argv, self._rows([[t, c, kind] for t in sorted(self.tables) for c, kind in self.tables[t]]))
        if "FROM pg_indexes" in statement:
            if "tablename = '" in statement:
                return self._ok(argv, "idx_old\n")
            return self._ok(argv, self._rows([[t, "idx_old"] for t in sorted(self.tables)]))
        raise AssertionError(f"unexpected psql query: {statement}")

    def _starrocks(self, argv):
        statement = argv[argv.index("-e") + 1]
        if "--skip-column-names" not in argv:
            self.executed.append(("starrocks", statement))
            if statement.startswith("ALTER TABLE"):
                table = statement.split("`.`")[1].split("`")[0]
                self.bloom[table] = set(statement.split('"bloom_filter_columns" = "')[1].split('"')[0].split(","))
            return self._ok(argv)
        if "FROM information_schema.tables" in statement:
            return self._ok(argv, self._rows([[t] for t in sorted(self.tables)]))
        if "FROM information_schema.columns" in statement:
            if "table_name = '" in statement:
                table = statement.split("table_name = '")[1].split("'")[0]
                return self._ok(argv, self._rows([[c, kind] for c, kind in self.tables.get(table, [])]))
            return self._ok(argv, self._rows([[t, c, kind] for t in sorted(self.tables) for c, kind in self.tables[t]]))
        if statement.startswith("SHOW ALTER TABLE COLUMN"):
            return self._ok(argv)
        rows = []
        for part in statement.split("; "):
            table = part.rsplit("`.`", 1)[1].rstrip("`")
            if part.startswith("SHOW INDEXES"):
                rows.append([table, "1", "idx_old"])
            elif part.startswith("SHOW CREATE TABLE"):
                columns = ",".join(sorted(self.bloom.get(table, ())))
                properties = f'"bloom_filter_columns" = "{columns}", ' if columns else ""
                rows.append([table, f"CREATE TABLE `{table}` (\\n...\\n) PROPERTIES ({properties}\"replication_num\" = \"1\")"])
            else:
                raise AssertionError(f"unexpected mysql query: {statement}")
        return self._ok(argv, self._rows(rows))

    def _mongo(self, argv):
        script = argv[argv.index("--eval") + 1]
        db_name = script.split("use('")[1].split("'")[0]
        if "_sdp_metadata" in script:
            return self._ok(argv, json.dumps(self.collections.get(db_name, [])))
        if "createIndex" in script:
            self.executed.append(("mongo", script))
            return self._ok(argv)
        if "findOne" in script:
            return self._ok(argv, json.dumps({"indexes": ["author_1"], "keys": ["_id", "id", "author", "score", "retrieved_on"]}))
        if "getIndexes" in script:
            return self._ok(argv, json.dumps(["author_1"]))
        raise AssertionError(f"unexpected mongosh script: {script}")


def write_deployment(root, auth=False, source="reddit", platform=PLATFORM):
    """A deployment with the three databases set up and one source loading into all of them."""
    for name, text in (("postgres", "port: 5432\nname: datasets\n"), ("mongo", "port: 27017\n"), ("starrocks", "port: 9030\n")):
        path = root / "config" / "db" / f"{name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    source_dir = root / "config" / "sources" / source
    source_dir.mkdir(parents=True)
    (source_dir / "platform.yaml").write_text(yaml.safe_dump(platform, sort_keys=False))
    (source_dir / "postgres.yaml").write_text("processing:\n  parallel_index_workers: 4\n")
    (source_dir / "mongo.yaml").write_text("{}\n")
    (source_dir / "starrocks.yaml").write_text("{}\n")
    flag = "true" if auth else "false"
    (root / ".env").write_text(f"POSTGRES_AUTH_ENABLED={flag}\nMONGO_AUTH_ENABLED={flag}\nSTARROCKS_AUTH_ENABLED={flag}\n")


def point_at(module, monkeypatch, root):
    """Point a CLI module and the setup helpers at the deployment in root."""
    config = root / "config"
    monkeypatch.setattr(module, "ROOT", root)
    monkeypatch.setattr(module, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "ROOT", root)
    monkeypatch.setattr(utils, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "SOURCES_DIR", config / "sources")


def drive_terminal(module, monkeypatch, database, answers, bitmap=(), save=False):
    """Run `db create-indexes --source reddit` with `answers` ({table: fields}, in the order the tables are picked) given at its questions; `bitmap` the "table.field" labels picked as BITMAP."""
    asked_lists = iter([answers[table] for table in answers])

    def multi_select(prompt, options, defaults=None, tag=None):
        if prompt.startswith("Select tables"):
            return [table for table in options if table in answers]
        if prompt.startswith("Which data types"):
            return [label for label in options if label.split(" (")[0] in answers]
        if prompt.startswith("Select BITMAP"):
            return [label for label in options if label in bitmap]
        raise AssertionError(prompt)

    label = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}[database]
    monkeypatch.setattr(utils, "ask_multi_select", multi_select)
    monkeypatch.setattr(utils, "ask_list", lambda prompt, default=None, tag=None: list(next(asked_lists)))
    monkeypatch.setattr(utils, "ask_choice", lambda prompt, choices, default=None, tag=None: label)
    monkeypatch.setattr(utils, "ask_bool", lambda prompt, default=None, tag=None: save)
    monkeypatch.setattr(utils, "section_header", lambda title: None)
    monkeypatch.setattr(module, "_prompt_db_password", lambda label="", tag=None: "made-up-admin-value")
    return module.cmd_db_create_indexes(argparse.Namespace(source="reddit"))


# The same answers, three times: for the terminal ({table: fields}, the BITMAP labels) and for the action (its `indexes`).
SCENARIOS = {
    "postgres": ({"comments": ["author", "score"], "submissions": ["title"]}, (), [{"table": "comments", "columns": ["author", "score"]}, {"table": "submissions", "columns": ["title"]}]),
    "mongo": ({"comments": ["author", "retrieved_on"]}, (), [{"table": "comments", "columns": ["author", "retrieved_on"]}]),
    # dataset already has a bloom filter (kept), score is chosen as BITMAP, controversiality (TINYINT) is BITMAP whatever is chosen, upvote_ratio (DOUBLE) cannot be indexed.
    "starrocks": (
        {"comments": ["author", "score", "controversiality", "upvote_ratio"], "comments_lingua": ["lang"]}, ("comments.score",),
        [{"table": "comments", "columns": ["author", "score", "controversiality", "upvote_ratio"], "bitmap": ["score"]}, {"table": "comments_lingua", "columns": ["lang"]}],
    ),
}


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "project"
    write_deployment(root)
    point_at(sdp, monkeypatch, root)
    # The StarRocks waits poll SHOW ALTER TABLE COLUMN, which the stand-in answers empty: they end at once. A wait that did not would sleep here.
    monkeypatch.setattr("time.sleep", lambda seconds: pytest.fail("a wait slept"))
    return root


def _fake(monkeypatch, module=sdp, **kwargs):
    fake = FakeDatabases(**kwargs)
    monkeypatch.setattr(module.subprocess, "run", fake.run)
    return fake


class TestSameStatementsAsTheTerminal:
    @pytest.mark.parametrize("database", sorted(SCENARIOS))
    def test_the_action_runs_what_the_terminal_runs_for_the_same_answers(self, deployment, monkeypatch, capsys, database):
        """Bug class: the web UI's index build drifting from the terminal's: another index name, another statement (the StarRocks bloom filter list that replaces the property, the PostgreSQL session settings), another order, a column type rule the terminal applies (DOUBLE skipped, TINYINT forced to BITMAP) left out, or a different platform.yaml when the answer to "Save" is yes. Both run against the same stand-in databases and their statements are compared whole."""
        answers, bitmap, indexes = SCENARIOS[database]
        platform_path = deployment / "config" / "sources" / "reddit" / "platform.yaml"
        terminal = _fake(monkeypatch)
        assert drive_terminal(sdp, monkeypatch, database, answers, bitmap, save=True) == 0
        saved_by_terminal = platform_path.read_text()
        platform_path.write_text(yaml.safe_dump(PLATFORM, sort_keys=False))

        action = _fake(monkeypatch)
        assert sdp.do_db_create_indexes(source="reddit", database=database, indexes=indexes, save=True) == 0

        assert terminal.executed and action.executed == terminal.executed
        assert platform_path.read_text() == saved_by_terminal

    def test_the_starrocks_statements_are_the_ones_expected(self, deployment, monkeypatch, capsys):
        """Bug class: both paths wrong in the same way (the comparison above would pass). The StarRocks statements written out: the bloom filter list keeps the column already set, the DOUBLE column is left out with a line saying so, the TINYINT one and the one chosen get a BITMAP index."""
        fake = _fake(monkeypatch)
        sdp.do_db_create_indexes(source="reddit", database="starrocks", indexes=SCENARIOS["starrocks"][2])

        assert sorted(fake.executed) == sorted([
            ("starrocks", 'ALTER TABLE `reddit`.`comments` SET ("bloom_filter_columns" = "author,dataset")'),
            ("starrocks", "CREATE INDEX `idx_comments_controversiality` ON `reddit`.`comments` (`controversiality`) USING BITMAP"),
            ("starrocks", "CREATE INDEX `idx_comments_score` ON `reddit`.`comments` (`score`) USING BITMAP"),
            ("starrocks", 'ALTER TABLE `reddit`.`comments_lingua` SET ("bloom_filter_columns" = "lang")'),
        ])
        out = capsys.readouterr().out
        assert "Skipping comments.upvote_ratio: StarRocks cannot index DOUBLE columns with either mechanism." in out
        assert "comments.controversiality is TINYINT — not bloom-filterable, using BITMAP." in out

    def test_without_save_platform_yaml_is_not_written(self, deployment, monkeypatch, capsys):
        """Bug class: the build saving its indexes into the source's config when the operator did not ask (the terminal's default answer is no)."""
        _fake(monkeypatch)
        platform_path = deployment / "config" / "sources" / "reddit" / "platform.yaml"
        before = platform_path.read_text()

        sdp.do_db_create_indexes(source="reddit", database="postgres", indexes=SCENARIOS["postgres"][2])

        assert platform_path.read_text() == before


class TestRefusedBeforeAnythingRuns:
    @pytest.mark.parametrize("database,indexes", [
        ("postgres", [{"table": "comments", "columns": ["author", "no_such_column"]}]),
        ("starrocks", [{"table": "comments", "columns": ["author"]}, {"table": "submissions", "columns": ["body"]}]),
        ("mongo", [{"table": "comments", "columns": ["not_a_field"]}]),
        ("postgres", [{"table": "no_such_table", "columns": ["id"]}]),
        ("mongo", [{"table": "no_such_type", "columns": ["id"]}]),
    ])
    def test_a_name_the_database_does_not_have_is_refused(self, deployment, monkeypatch, capsys, database, indexes):
        """Bug class: an index field the database does not have (typed past the form, or dropped from the table since the page was drawn) handed to `CREATE INDEX` or `createIndex`: the registry's stated blocker for this verb. The action reads the real columns itself and refuses before any statement runs, the valid tables of the same request included."""
        fake = _fake(monkeypatch)

        with pytest.raises(sdp.ActionError) as exc:
            sdp.do_db_create_indexes(source="reddit", database=database, indexes=indexes)

        assert exc.value.message in (sdp._INDEX_NO_COLUMN, sdp._INDEX_NO_TABLE)
        assert fake.executed == []

    @pytest.mark.parametrize("database,indexes,message", [
        ("postgres", [{"table": "comments", "columns": ["author"], "bitmap": ["author"]}], sdp._INDEX_BITMAP_NOT_HERE),
        ("starrocks", [{"table": "comments", "columns": ["author"], "bitmap": ["score"]}], sdp._INDEX_BITMAP_NOT_A_COLUMN),
        ("postgres", [{"table": "comments", "columns": ["author"]}, {"table": "comments", "columns": ["score"]}], sdp._INDEX_TABLE_TWICE),
        ("postgres", [], sdp._INDEX_REQUEST_EMPTY),
        ("postgres", [{"table": "comments", "columns": ["author; DROP TABLE x"]}], sdp._INDEX_REQUEST_NOT_VALID),
        ("mongo", [{"table": "comments", "columns": ["a'}); db.dropDatabase(); ({'"]}], sdp._INDEX_REQUEST_NOT_VALID),
    ])
    def test_a_request_of_the_wrong_shape_is_refused_unread(self, deployment, monkeypatch, capsys, database, indexes, message):
        """Bug class: the action trusting its caller for the shape the registry checks (it is a function of the CLI module and can be called without the registry): a name that would close the quoted identifier or the script string it is pasted into, a bitmap choice where there is no such index type. Refused before the database is asked anything."""
        fake = _fake(monkeypatch)

        with pytest.raises(sdp.ActionError) as exc:
            sdp.do_db_create_indexes(source="reddit", database=database, indexes=indexes)

        assert exc.value.message == message
        assert fake.seen == []

    def test_with_authentication_on_and_no_password_nothing_is_asked(self, tmp_path, monkeypatch, capsys):
        """Bug class: an action that waits on a terminal it does not have, or asks the database without the password it needs. With the password given it reaches psql through PGPASSWORD, never the command line."""
        root = tmp_path.resolve() / "project"
        write_deployment(root, auth=True)
        point_at(sdp, monkeypatch, root)
        fake = _fake(monkeypatch)
        envs = []
        recording = fake.run

        def run(argv, **kwargs):
            envs.append(kwargs.get("env") or {})
            return recording(argv, **kwargs)

        monkeypatch.setattr(sdp.subprocess, "run", run)

        with pytest.raises(sdp.ActionError) as exc:
            sdp.do_db_create_indexes(source="reddit", database="postgres", indexes=SCENARIOS["postgres"][2])
        assert exc.value.message == sdp._ADMIN_PASSWORD_REQUIRED and fake.seen == []

        sdp.do_db_create_indexes(source="reddit", database="postgres", indexes=SCENARIOS["postgres"][2], admin_password="made-up-admin-value")
        assert fake.executed and all(env.get("PGPASSWORD") == "made-up-admin-value" for env in envs)
        assert not any("made-up-admin-value" in " ".join(argv) for argv in fake.seen)


class TestListing:
    def test_the_listing_offers_the_real_columns_and_existing_indexes(self, deployment, monkeypatch):
        """Bug class: a picker that offers what the database does not have, or hides what it has: the tables come from the database, each with its columns and types, the existing indexes, StarRocks's bloom filter columns and which index types each column type takes; a MongoDB data type with its configured fields and the keys of a stored document."""
        _fake(monkeypatch)

        starrocks = sdp.get_db_index_targets(source="reddit", db="starrocks")
        mongo = sdp.get_db_index_targets(source="reddit", db="mongo")

        comments = next(t for t in starrocks["tables"] if t["name"] == "comments")
        assert [t["name"] for t in starrocks["tables"]] == ["comments", "comments_lingua", "submissions"]
        assert [(c["name"], c["type"], c["bloom"], c["bitmap"]) for c in comments["columns"]][-3:] == [("score", "int", True, True), ("controversiality", "tinyint", False, True), ("upvote_ratio", "double", False, False)]
        assert (comments["indexes"], comments["bloom_filter"], starrocks["scope"]) == (["idx_old"], ["dataset"], "reddit")
        assert [(t["name"], t["db_name"], t["collections"], t["indexes"]) for t in mongo["tables"]] == [("submissions", "reddit_submissions", 1, ["author_1"]), ("comments", "reddit_comments", 2, ["author_1"])]
        assert [c["name"] for c in mongo["tables"][1]["columns"]] == ["dataset", "id", "author", "body", "subreddit", "score", "retrieved_on"]
        assert json.loads(json.dumps(starrocks)) == starrocks

    def test_the_listing_is_bounded_and_offers_only_plain_names(self, deployment, monkeypatch):
        """Bug class: a listing whose size grows with the database (a read has a time limit and the page a field limit), or that offers a name no request can carry (a quote, a space, a leading underscore): it would be picked and then refused, or pasted into a statement."""
        tables = {f"t{i:03d}": [("id", "int")] for i in range(sdp.INDEX_TARGETS_TABLES_MAX + 5)}
        tables["t000"] = [(f"c{i}", "int") for i in range(sdp.INDEX_TARGETS_COLUMNS_MAX + 3)] + [("bad name", "int"), ("_hidden", "int")]
        tables["bad`table"] = [("id", "int")]
        _fake(monkeypatch, tables=tables)

        data = sdp.get_db_index_targets(source="reddit", db="postgres")

        assert len(data["tables"]) == sdp.INDEX_TARGETS_TABLES_MAX and data["truncated"] is True
        assert len(data["tables"][0]["columns"]) == sdp.INDEX_TARGETS_COLUMNS_MAX
        assert data["skipped_tables"] == 1 and data["skipped_columns"] == 5
        assert data["parallel_workers"] == 4 and data["scope"] == "reddit"

    def test_the_names_left_out_are_named_bounded_and_display_safe(self, deployment, monkeypatch):
        """Bug class: a listing that says only how many tables and columns it left out, so the operator cannot tell which ones to index in a terminal; or one that names them without a bound (a schema of thousands) or with the database's control characters in them."""
        max_columns = sdp.INDEX_TARGETS_COLUMNS_MAX
        tables = {"t000": [(f"c{i}", "int") for i in range(max_columns + 2)] + [("bad name", "int"), ("_hidden\x1b[31m", "int")], "bad`table": [("id", "int")]}
        _fake(monkeypatch, tables=tables)

        data = sdp.get_db_index_targets(source="reddit", db="postgres")

        assert (data["skipped_tables"], data["skipped_columns"]) == (1, 4)
        assert data["skipped_names"] == ["bad`table", f"t000.c{max_columns}", f"t000.c{max_columns + 1}", "t000.bad name", "t000._hidden\\x1b[31m"]

        _fake(monkeypatch, tables={**{f"bad table {i:02d}": [("id", "int")] for i in range(sdp.INDEX_SKIPPED_NAMES_MAX + 5)}, "t": [("id", "int")]})
        data = sdp.get_db_index_targets(source="reddit", db="postgres")
        assert data["skipped_tables"] == sdp.INDEX_SKIPPED_NAMES_MAX + 5 and len(data["skipped_names"]) == sdp.INDEX_SKIPPED_NAMES_MAX == 20

    def test_a_database_the_source_does_not_use_is_refused(self, tmp_path, monkeypatch):
        """Bug class: a listing or a build for a database the source loads nothing into, or one not set up: `db create-indexes` offers only the source's configured databases."""
        root = tmp_path.resolve() / "project"
        write_deployment(root)
        (root / "config" / "sources" / "reddit" / "mongo.yaml").unlink()
        point_at(sdp, monkeypatch, root)
        fake = _fake(monkeypatch)

        with pytest.raises(sdp.ActionError) as exc:
            sdp.get_db_index_targets(source="reddit", db="mongo")

        assert exc.value.message == sdp._INDEX_DATABASE_NOT_USED and fake.seen == []


class TestTheRegistryRefuses:
    @pytest.mark.parametrize("fields,field", [
        ({"database": "postgres", "indexes": [{"table": "comments", "columns": ["author"], "bitmap": ["author"]}]}, "indexes"),
        ({"database": "starrocks", "indexes": [{"table": "comments", "columns": ["author"], "bitmap": ["score"]}]}, "indexes"),
        ({"database": "starrocks", "indexes": [{"table": "comments", "columns": ["author"]}, {"table": "comments", "columns": ["id"]}]}, "indexes"),
        ({"database": "postgres", "indexes": [{"table": "comments", "columns": ["author", "author"]}]}, "indexes"),
        ({"database": "postgres", "indexes": [{"table": "comments", "columns": ["a b"]}]}, "indexes"),
        ({"database": "mongo", "indexes": [{"table": "comments", "columns": ["x'}"]}]}, "indexes"),
        ({"database": "postgres", "indexes": [{"table": "comments", "columns": ["author"], "kind": "bloom"}]}, "indexes"),
        ({"database": "postgres", "indexes": [{"table": "comments"}]}, "indexes"),
        ({"database": "oracle", "indexes": [{"table": "comments", "columns": ["author"]}]}, "database"),
    ])
    def test_what_the_terminal_would_never_build(self, deployment, fields, field):
        """Bug class: a request the broker passes to the action although the terminal could never produce it: a name that is not a plain identifier (it is pasted into SQL and a mongosh script), a column twice, an unknown key, a BITMAP choice outside StarRocks or outside the table's columns, a table twice, a database there is none of."""
        from social_data_pipeline.control import errors, verbs

        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request("db.create_indexes", {"source": "reddit", **fields})

        assert exc.value.field == field

    def test_a_request_the_terminal_could_answer_passes(self, deployment):
        """Bug class: a registry stricter than the terminal, so a build the terminal does cannot be asked for."""
        from social_data_pipeline.control import verbs

        validated = verbs.validate_request("db.create_indexes", {"source": "reddit", "database": "starrocks", "indexes": SCENARIOS["starrocks"][2], "save": True})

        assert validated["indexes"] == SCENARIOS["starrocks"][2]
