"""Tests for `webui.statements`, which need no FastAPI: how the Queue window shows a statement and which hints it gives.

The contracts pinned here, each test naming the bug class it catches:

- c (the statement part). Every character of the categories Cc (but the line break and the tab), Cf, Zl, Zp, Cs and Co is made a visible escape, and counted; the rest is kept exactly.
- f. The hints: each word of each backend's list is found, case-insensitively, also across a line break; the rule of the semicolon that is not the last character; PostgreSQL's leading ANALYZE or option list (after comments too); words inside other words are not hints; an unknown backend gets none; a megabyte of text is scanned in steps proportional to its length (counted, not timed), for the inputs that make the most work.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.webui import statements


@pytest.mark.parametrize("character, escape", [
    ("\u202e", "\\u202e"), ("\u2066", "\\u2066"), ("\u200b", "\\u200b"), ("\x00", "\\u0000"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029"),
    ("\ud800", "\\ud800"), ("\ue000", "\\ue000"), ("\U000f0001", "\\U000f0001"), ("\x1b", "\\u001b"), ("\r", "\\u000d"), ("\x85", "\\u0085"), ("\ufeff", "\\ufeff"), ("\U000e0041", "\\U000e0041"),
])
def test_each_invisible_or_control_character_is_an_escape(character, escape):
    """Bug class: a statement that reads as something else than it holds (a direction change that reorders it, an invisible character between the letters of a word, a line separator), or one that cannot be written to the page at all (a lone surrogate), shown to the operator who approves it."""
    parts, count = statements.visible("SELECT " + character + "1")
    assert parts == [(False, "SELECT "), (True, escape), (False, "1")] and count == 1
    assert statements.shown_text("a" + character) == "a" + escape


def test_line_breaks_tabs_and_ordinary_text_are_kept_exactly():
    """Bug class: the statement shown changed where nothing needed showing (its line breaks, tabs, letters of other scripts, a backslash that looks like an escape), so that what is shown is not what runs."""
    text = "SELECT\n\t'\u00e9\u4e2d \\u202e'\n"
    assert statements.visible(text) == ([(False, text)], 0)
    parts, count = statements.visible("\u202e\u2066x\u200b")
    assert parts == [(True, "\\u202e\\u2066"), (False, "x"), (True, "\\u200b")] and count == 3
    assert statements.visible("") == ([], 0) and statements.visible(None) == ([], 0)


HINT_CASES = [
    ("postgres", "COPY t TO PROGRAM 'x'", ["PROGRAM", "COPY"]),
    ("postgres", "select Pg_Read_File('x')", ["pg_read_"]),
    ("postgres", "select pg_ls_dir('.')", ["pg_ls_"]),
    ("postgres", "select pg_write_x()", ["pg_write_"]),
    ("postgres", "select LO_IMPORT('x'), lo_export(1, 'y')", ["lo_import", "lo_export"]),
    ("postgres", "select * from DBLINK('x', 'y')", ["dblink"]),
    ("starrocks", "SELECT 1 INTO\n  OUTFILE 'x'", ["INTO OUTFILE"]),
    ("starrocks", "select * from files('x')", ["FILES("]),
    ("starrocks", "select * from FILES ('x')", ["FILES("]),
    ("starrocks", "Insert into t values (1)", ["INSERT"]),
    ("starrocks", "LOAD LABEL x", ["LOAD"]),
    ("starrocks", "set\tglobal x = 1", ["SET"]),
    ("mongodb", '[{"$OUT": "x"}]', ["$out"]),
    ("mongodb", '[{"$merge": {"into": "x"}}]', ["$merge"]),
    ("mongodb", '[{"$project": {"a": {"$function": {}}}}]', ["$function"]),
    ("mongodb", '[{"$group": {"a": {"$accumulator": {}}}}]', ["$accumulator"]),
    ("mongodb", '[{"$match": {"$where": "1"}}]', ["$where"]),
]


@pytest.mark.parametrize("backend, text, found", HINT_CASES)
def test_each_word_is_found_case_insensitively(backend, text, found):
    """Bug class: a word of the list missed because of its case, or because the statement breaks a phrase over a line or puts a space before a parenthesis."""
    assert statements.hints(text, backend) == found


@pytest.mark.parametrize("backend, text", [
    ("postgres", "SELECT copyright, programme FROM t"),
    ("starrocks", "SELECT a FROM t ORDER BY a LIMIT 1 OFFSET 2"),
    ("starrocks", "SELECT reload, inserted, settings FROM t"),
    ("mongodb", '[{"$outer": 1, "$merged": 2}]'),
    ("postgres", "SELECT 1"),
    ("made-up", "COPY t TO PROGRAM 'x'; select 2"),
])
def test_words_inside_other_words_and_other_backends_are_no_hints(backend, text):
    """Bug class: hints for every query that has an OFFSET or a column named like a word (the operator learns to ignore them), or the words of another backend applied."""
    assert statements.hints(text, backend) == []


@pytest.mark.parametrize("backend, text, hinted", [
    ("postgres", "SELECT 1;", False),
    ("postgres", "SELECT 1;  \n\t", False),
    ("postgres", "SELECT 1; SELECT 2", True),
    ("postgres", "SELECT 1;;", True),
    ("starrocks", "SELECT 1; DROP TABLE t;", True),
    ("starrocks", "SELECT 1", False),
    ("mongodb", '[{"$match": {"a": ";"}}, 1]', False),
])
def test_a_semicolon_that_is_not_the_last_character(backend, text, hinted):
    """Bug class: the hint for a second statement missing (the scheduler strips only trailing semicolons and sends the rest), or given for the one trailing semicolon every query may have."""
    assert (statements.SEMICOLON_HINT in statements.hints(text, backend)) is hinted


@pytest.mark.parametrize("text, hinted", [
    ("ANALYZE SELECT 1", True),
    ("  analyse select 1", True),
    ("(ANALYZE, BUFFERS) SELECT 1", True),
    ("-- a comment\n/* another */ analyze select 1", True),
    ("SELECT analyze FROM t", False),
    ("SELECT 1", False),
])
def test_postgres_leading_analyze_or_option_list(text, hinted):
    """Bug class: a statement that makes the EXPLAIN button run the query (EXPLAIN ANALYZE) without a hint, also when a comment comes first."""
    assert (statements.LEADING_HINT in statements.hints(text, "postgres")) is hinted
    assert statements.LEADING_HINT not in statements.hints(text, "starrocks")


@pytest.mark.parametrize("backend, unit", [
    ("postgres", "p "), ("postgres", "pg_"), ("postgres", "l "), ("starrocks", "i "), ("starrocks", "f "), ("mongodb", "$ "), ("postgres", "a;"), ("postgres", "-- x\n"), ("starrocks", "x"),
])
def test_a_megabyte_is_scanned_in_linear_steps(backend, unit):
    """Bug class: a scan whose cost grows faster than the statement (a search started again at every position, a backtracking pattern): a statement of a megabyte, which the scheduler takes, would then stall the page. The steps are counted for one and for two megabytes of the input that gives the most candidates."""
    counted = []
    for size in (1024 * 1024, 2 * 1024 * 1024):
        steps = []
        statements.hints(unit * (size // len(unit)), backend, steps=steps)
        counted.append((size, steps[0]))
    (one, first), (two, second) = counted
    assert first <= 8 * one and second <= 8 * two
    assert second <= 2.2 * first


# ── round 2 ─────────────────────────────────────────────────────────────────


def test_a_lone_surrogate_is_an_escape():
    """Bug class: a lone surrogate (which the scheduler's socket replaces by `?` before it sends, but which a record may hold) written into a page, which cannot encode it."""
    assert statements.visible("a\ud800b") == ([(False, "a"), (True, "\\ud800"), (False, "b")], 1)


@pytest.mark.parametrize("text, hinted", [
    ("/* a /* nested */ comment */ ANALYZE SELECT 1", True),
    ("/* /* */ */ (ANALYZE) SELECT 1", True),
    ("/* /* */ ANALYZE SELECT 1", False),
    ("/* never closed ANALYZE SELECT 1", False),
    ("/**/ analyze select 1", True),
])
def test_postgres_nests_its_block_comments(text, hinted):
    """Bug class: a leading ANALYZE hidden behind nested block comments, which PostgreSQL reads as one comment (so EXPLAIN runs the statement) and a scan for the first `*/` would end too early."""
    assert (statements.LEADING_HINT in statements.hints(text, "postgres")) is hinted


@pytest.mark.parametrize("text, found", [
    ("SELECT $q$x$q$; COPY t TO PROGRAM 'x'", ["PROGRAM", "COPY", statements.SEMICOLON_HINT]),
    ("DO $q$COPY t TO PROGRAM 'x'$q$", ["PROGRAM", "COPY"]),
    ("SELECT $$lo_import$$", ["lo_import"]),
])
def test_a_dollar_quote_does_not_hide_a_postgres_word(text, found):
    """Bug class: a word of the list inside a dollar-quoted body (`$q$COPY ...$q$`) missed because `$` was taken for a letter of the word before it."""
    assert statements.hints(text, "postgres") == found


@pytest.mark.parametrize("text, found", [
    ('[{"\\u0024out": "x"}]', ["$out"]),
    ('[{"\\U0024MERGE": {}}]', ["$merge"]),
    ('[{"$match": {"\\u0024where": "1"}}, {"\\u0024function": 1}, {"\\u0024accumulator": 1}]', ["$function", "$accumulator", "$where"]),
    ('[{"\\u0024outer": 1}]', []),
])
def test_a_mongodb_operator_written_with_an_escaped_dollar_is_found(text, found):
    """Bug class: `$out` written in the pipeline's JSON as `\\u0024out`, which the database reads as `$out`, not hinted."""
    assert statements.hints(text, "mongodb") == found


@pytest.mark.parametrize("backend, unit", [("postgres", "/*"), ("postgres", "/* */"), ("postgres", "$q$ "), ("mongodb", "\\u0024 "), ("mongodb", "\\u0024out ")])
def test_the_new_rules_stay_linear(backend, unit):
    """Bug class: the nesting of comments or the escaped dollar making the scan's cost grow faster than the statement."""
    counted = []
    for size in (1024 * 1024, 2 * 1024 * 1024):
        steps = []
        statements.hints(unit * (size // len(unit)), backend, steps=steps)
        counted.append((size, steps[0]))
    (one, first), (two, second) = counted
    assert first <= 8 * one and second <= 8 * two and second <= 2.2 * first
