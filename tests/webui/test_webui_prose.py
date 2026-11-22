"""Tests for `webui.prose`, the renderer of the assistant's text into blocks of data.

The contracts pinned here, each test naming the bug class it catches:

- e. Each construct (paragraph, the three headings, bullet and numbered lists, a fenced block, a quote, bold, italic, both, code); the nesting limits (one level of lists, code holding no emphasis); markers that are not closed stay text; links, images and raw HTML are their literal text; the input is cut at 64 KiB; every text run passes the visible escapes; the steps are linear in the input for adversarial texts (30,000 asterisks, backticks or brackets, unclosed fences, nested markers, a list item with tens of thousands of continuation lines).

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.webui import prose


def text_of(runs):
    return "".join(part for run in runs for _escape, part in run["parts"])


def styles(runs):
    return [("b" if run["bold"] else "") + ("i" if run["italic"] else "") + ("c" if run["code"] else "") + ":" + "".join(part for _e, part in run["parts"]) for run in runs]


def test_each_block_construct():
    """Bug class: a construct drawn as another (a heading as a paragraph, a list item as text), a line break of a paragraph lost, a fence's content parsed as prose."""
    text = "# One\n## Two\n### Three\n#### Four\nfirst line\nsecond line\n\n- a\n* b\n  more of b\n1. x\n2) y\n> quoted\n> on\n```python\n**not bold** <b>\n```\nend"
    found, cut = prose.blocks(text)
    assert not cut
    assert [(block["kind"], block.get("level")) for block in found] == [("h", 1), ("h", 2), ("h", 3), ("h", 3), ("p", None), ("ul", None), ("ol", None), ("quote", None), ("code", None), ("p", None)]
    assert text_of(found[3]["runs"]) == "Four" and text_of(found[4]["runs"]) == "first line\nsecond line"
    assert [text_of(item) for item in found[5]["items"]] == ["a", "b\nmore of b"] and [text_of(item) for item in found[6]["items"]] == ["x", "y"]
    assert text_of(found[7]["runs"]) == "quoted\non"
    assert "".join(part for _e, part in found[8]["parts"]) == "**not bold** <b>"


def test_an_unclosed_fence_runs_to_the_end():
    """Bug class: an unclosed fence dropping its content or parsing it as prose."""
    found, _ = prose.blocks("before\n```\n# not a heading\n*x*")
    assert [block["kind"] for block in found] == ["p", "code"] and "".join(part for _e, part in found[1]["parts"]) == "# not a heading\n*x*"


@pytest.mark.parametrize("text, expected", [
    ("a **b** c", [":a ", "b:b", ": c"]),
    ("a *b* c", [":a ", "i:b", ": c"]),
    ("***both***", ["bi:both"]),
    ("**bold *and italic***", ["b:bold ", "bi:and italic"]),
    ("use `x **y**` here", [":use ", "c:x **y**", ": here"]),
    ("**use `x` here**", ["b:use ", "bc:x", "b: here"]),
    ("2 * 3 * 4", [":2 * 3 * 4"]),
    ("**never closed", [":**never closed"]),
    ("*a *b", [":*a *b"]),
    ("`unpaired", [":`unpaired"]),
    ("empty `` pair", [":empty `` pair"]),
    ("snake_case_name", [":snake_case_name"]),
])
def test_inline_runs(text, expected):
    """Bug class: emphasis that swallows the rest of the text when a marker is not closed, a lone `*` of arithmetic read as emphasis, code that holds emphasis, underscores of names read as emphasis."""
    assert styles(prose.runs(text)) == expected


@pytest.mark.parametrize("text", [
    "[a link](javascript:alert(1))",
    "![an image](https://example.invalid/x.png)",
    "<img src=x onerror=alert(1)><script>alert(1)</script>",
    "<https://example.invalid>",
])
def test_links_images_and_html_are_literal_text(text):
    """Bug class: a link, an image or raw HTML of the assistant's text turned into an element (which could leave the page or run code)."""
    found, _ = prose.blocks(text)
    assert len(found) == 1 and styles(found[0]["runs"]) == [":" + text]


def test_invisible_characters_are_escapes_in_every_run():
    """Bug class: a direction change or an invisible character of the assistant's text shown as itself, in a run or in a fenced block."""
    found, _ = prose.blocks("a\u202eb **c\u200bd**\n```\ne\u2066f\n```")
    runs = found[0]["runs"]
    assert (True, "\\u202e") in runs[0]["parts"] and (True, "\\u200b") in runs[1]["parts"]
    assert (True, "\\u2066") in found[1]["parts"]


def test_the_input_is_cut_at_64_kib():
    """Bug class: an unbounded text rendered whole (a slow page, a large model), or a cut inside a character."""
    found, cut = prose.blocks("\u00e9" * 40000)
    assert cut and len(text_of(found[0]["runs"]).encode("utf-8")) == prose.TEXT_MAX_BYTES


@pytest.mark.parametrize("unit", ["*", "`", "[", "**a", "*a ", "`a", "# ", "- ", "> ", "```\n", "1. ", "\n", "**`*", "[x](", "<b>"])
def test_the_steps_are_linear(unit):
    """Bug class: a renderer whose work grows faster than its input (a backtracking pattern, a search for a closer from every opener), which a text of the assistant can make take minutes."""
    measured = []
    for count in (3000, 30000):
        steps = []
        prose.blocks(unit * count, steps)
        measured.append(steps[0] / (len(unit) * count))
    assert measured[1] <= measured[0] * 1.5 + 1 and measured[1] < 12, measured


def test_a_list_item_with_many_continuation_lines_is_linear():
    """Bug class: a list item whose text is copied again for every continuation line (quadratic in the item's length), which a long indented answer makes take minutes."""
    measured = []
    for count in (2000, 20000):
        steps = []
        found, cut = prose.blocks("- a\n" + " b\n" * count, steps)
        assert not cut and [block["kind"] for block in found] == ["ul"] and text_of(found[0]["items"][0]) == "a" + "\nb" * count
        measured.append(steps[0] / (3 * count))
    assert measured[1] <= measured[0] * 1.5 + 1 and measured[1] < 12, measured
