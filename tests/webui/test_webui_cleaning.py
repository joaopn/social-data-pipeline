"""Tests for `webui.cleaning.clean_log_text`, which makes a program's output fit to show: each rule, and a cost linear in the length whatever the text holds (counted in character visits, not timed).

Every special character is written as an escape.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.webui.cleaning import clean_log_text


@pytest.mark.parametrize("raw, shown", [
    ("\x1b[31mred\x1b[0m plain", "red plain"),
    ("a\x1b[1;32;48;5;200mb", "ab"),
    ("a\x1b[?25lb\x1b[?25h", "ab"),
    ("a\x9b31mb", "ab"),
    ("a\x1b[2 qb", "ab"),
])
def test_csi_sequences_go_whole(raw, shown):
    """Bug class: colour and cursor codes shown as `[31m` litter, or only their ESC removed so that `[1;32m` stays."""
    assert clean_log_text(raw) == shown


@pytest.mark.parametrize("raw, shown", [
    ("a\x1b]0;window title\x07b", "ab"),
    ("a\x1b]8;;https://example.invalid/\x1b\\link\x1b]8;;\x1b\\b", "alinkb"),
    ("a\x9d0;title\x9cb", "ab"),
    ("a\x1bPdevice control\x1b\\b", "ab"),
    ("a\x1b_application\x07b", "ab"),
    ("a\x1b^privacy\x07b", "ab"),
    ("a\x1bXstring\x07b", "ab"),
])
def test_osc_and_other_strings_go_up_to_their_end(raw, shown):
    """Bug class: an OSC (a terminal title, a hyperlink whose target differs from its text) left in the page, or its payload shown as text."""
    assert clean_log_text(raw) == shown


def test_an_unended_string_stops_at_the_line_break():
    """Bug class: one stray `ESC ]` hiding the whole rest of a log."""
    assert clean_log_text("a\x1b]never ended\nnext line") == "a\nnext line"


@pytest.mark.parametrize("raw, shown", [
    ("a\x1b(Bb", "ab"),
    ("a\x1b7b\x1b8", "ab"),
    ("a\x1bcb", "ab"),
    ("a\x1b", "a"),
    ("a\x1b\x1b[31mb", "ab"),
])
def test_other_escapes_go(raw, shown):
    """Bug class: charset switches, cursor saves and resets left as letters in the text."""
    assert clean_log_text(raw) == shown


@pytest.mark.parametrize("raw, shown", [
    ("10%\r20%\r30%", "30%"),
    ("downloading 10%\rdownloading 99%\ndone", "downloading 99%\ndone"),
    ("line one\r\nline two\r\n", "line one\nline two\n"),
    ("kept\r\n", "kept\n"),
    ("kept\r", "kept"),
    ("old\r\x1b[2Knew", "new"),
])
def test_a_lone_carriage_return_means_the_line_is_written_again(raw, shown):
    """Bug class: a progress bar shown as all its states joined on one line (or a megabyte of them), a CRLF line ending treated as an overwrite, or a last line lost to a carriage return that nothing follows."""
    assert clean_log_text(raw) == shown


def test_control_characters_go_but_line_feed_and_tab_stay():
    """Bug class: a bell, a backspace, NUL or a C1 control reaching the page (a backspace can make a line read as something else); or tabs and line breaks lost, which flattens the log."""
    raw = "a\x00b\x07c\x08d\x7fe\x85f\x0bg\x0ch\x1ci\tj\nk"
    assert clean_log_text(raw) == "abcdefghi\tj\nk"


@pytest.mark.parametrize("character", ["\u202a", "\u202b", "\u202c", "\u202d", "\u202e", "\u2066", "\u2067", "\u2068", "\u2069"])
def test_direction_changing_characters_go(character):
    """Bug class: a right-to-left override that makes `exe.txt` read as `txt.exe` (or a log line read backwards) on the page."""
    assert clean_log_text(f"name{character}txt.exe") == "nametxt.exe"


def test_other_text_is_kept_as_it_is():
    """Bug class: a cleaner that also drops letters of other scripts, symbols or markup characters (escaping is the template's job, not this one's)."""
    raw = "caf\u00e9 \u4e2d\u6587 <b>&amp;</b> \"quotes\" 'single' \u2192 \u00a0end"
    assert clean_log_text(raw) == raw


HOSTILE_SHAPES = {
    "csi without a final": lambda n: "\x1b[" + "1;" * n,
    "csi starts": lambda n: "\x1b[" * n,
    "osc starts": lambda n: "\x1b]" * n,
    "unended osc": lambda n: "\x1b]" + "x" * n,
    "escape pairs": lambda n: "\x1b\x1b" * n,
    "carriage returns": lambda n: "ab\r" * n,
    "rewrites of a long line": lambda n: "x" * n + "\ry" * n,
    "c1 csi": lambda n: "\x9b" * n,
    "string terminators": lambda n: "\x1b\\" * n,
    "line breaks in strings": lambda n: "\x1b]a\n" * n,
}


@pytest.mark.parametrize("shape", sorted(HOSTILE_SHAPES))
def test_the_cost_is_linear_for_hostile_shapes(shape):
    """Bug class: a scan that goes back over the text (a regex with backtracking, a rescan after each escape or each carriage return) and turns a log of a few megabytes into minutes of work for the page. Counted in character visits at three sizes."""
    ratios = []
    for size in (1000, 10000, 100000):
        text = HOSTILE_SHAPES[shape](size)
        steps = []
        clean_log_text(text, steps)
        ratios.append(steps[0] / len(text))
    assert max(ratios) <= 3
    assert ratios[2] <= ratios[0] * 1.5 + 0.01
