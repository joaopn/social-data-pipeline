"""Tests for the check of a chart spec before the Chat window sends it to the page (`webui.vlspec`): the spec is the sandbox's output, so hostile.

The contracts pinned here, each test naming the bug class it catches: a spec Altair writes passes whole; data loaded from an address, a mark's link, an image mark, a data loader, a control placed elsewhere in the page and a prototype key are refused wherever they sit; data only inline and bounded in rows; views bounded; strict JSON (NaN, Infinity, out-of-range numbers, repeated keys), bounded depth, size and count of containers, the depth counted without recursion and outside strings only; a Vega (not Vega-Lite) spec refused; `usermeta` dropped.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from social_data_pipeline.webui import vlspec

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "webui" / "altair_line.vl.json"


@pytest.fixture
def altair():
    """A spec Altair 6.3.0 wrote (`chart.save`): two layers, data in `datasets`, a legend selection, zoom and pan bound to the scales, tooltips."""
    return json.loads(FIXTURE.read_text())


def checked(spec):
    text, reason = vlspec.check(json.dumps(spec).encode())
    return json.loads(text) if text is not None else reason


def test_a_spec_altair_wrote_passes_whole(altair):
    """Bug class: a check so strict it refuses the charts the assistant is told to make (Altair's named datasets, its `$schema`, its params), or one that changes them."""
    text, reason = vlspec.check(FIXTURE.read_bytes())
    assert reason is None and json.loads(text) == altair
    assert altair["data"] == {"name": next(iter(altair["datasets"]))} and len(next(iter(altair["datasets"].values()))) == 30


def with_change(spec, path, value):
    """A copy of `spec` with `value` set at `path` (keys and list indexes)."""
    changed = copy.deepcopy(spec)
    place = changed
    for step in path[:-1]:
        place = place[step]
    place[path[-1]] = value
    return changed


@pytest.mark.parametrize("path, value, reason", [
    (("data",), {"url": "/chat/c_0123456789ab/file/secret.csv"}, vlspec.REFUSED_KEYS["url"]),
    (("layer", 1, "data"), {"url": "https://example.org/x.json"}, vlspec.REFUSED_KEYS["url"]),
    (("transform",), [{"lookup": "source", "from": {"data": {"url": "/queue"}, "key": "source", "fields": ["x"]}}], vlspec.REFUSED_KEYS["url"]),
    (("data",), {"values": [], "format": {"type": "topojson", "url": "x"}}, vlspec.REFUSED_KEYS["url"]),
    (("layer", 0, "encoding", "href"), {"field": "source"}, vlspec.REFUSED_KEYS["href"]),
    (("layer", 0, "mark", "href"), "https://example.org/?leak", vlspec.REFUSED_KEYS["href"]),
    (("config", "mark"), {"href": "https://example.org/"}, vlspec.REFUSED_KEYS["href"]),
    (("layer", 1, "mark"), "image", vlspec.IMAGE),
    (("layer", 1, "mark"), {"type": "image", "width": 10}, vlspec.IMAGE),
    (("layer", 0, "encoding", "url"), {"field": "source"}, vlspec.REFUSED_KEYS["url"]),
    (("config", "loader"), {"baseURL": "https://example.org/"}, vlspec.REFUSED_KEYS["loader"]),
    (("params", 0, "bind"), {"input": "text", "element": "#chat-form"}, vlspec.REFUSED_KEYS["element"]),
    (("datasets",), {"a": "https://example.org/x.csv"}, vlspec.NOT_INLINE),
    (("data",), {"name": "not-held"}, vlspec.NO_DATASET),
    (("data",), {"values": "a,b\n1,2", "format": {"type": "csv"}}, vlspec.NOT_INLINE),
    (("data",), {"sequence": {"start": 0, "stop": 10 ** 9}}, vlspec.TOO_MANY_ROWS),
    (("data",), {"source": "x"}, vlspec.NOT_INLINE),
    (("$schema",), "https://vega.github.io/schema/vega/v6.json", vlspec.NOT_VEGA_LITE),
    (("$schema",), "https://example.org/schema/vega-lite/v6.json", vlspec.NOT_VEGA_LITE),
    (("layer", 0, "$schema"), "https://vega.github.io/schema/vega-lite/v6.json", vlspec.NOT_VEGA_LITE),
    (("layer", 0, "encoding", "__proto__"), {"x": 1}, vlspec.PROTOTYPE),
])
def test_what_would_reach_out_or_out_of_the_chart_is_refused_wherever_it_sits(altair, path, value, reason):
    """Bug class: a spec that makes the browser read this web UI's own routes with the operator's cookie (data from an address), navigate away with data in a link (`href`), load images, set the library's loader, put a control into the page's own forms (`element`), pollute an object's prototype, or pass as a Vega spec, because the check looked only at the top level."""
    assert checked(with_change(altair, path, value)) == reason


def test_a_url_inside_the_data_rows_is_data(altair):
    """Bug class: a chart refused because a column of its data is named `url` or holds an address (rows are drawn as text, never fetched)."""
    rows = next(iter(altair["datasets"].values()))
    rows[0]["url"] = "https://example.org/"
    rows[1]["href"] = "https://example.org/"
    assert checked(altair) == altair


def test_a_prototype_key_in_the_rows_is_refused(altair):
    """Bug class: a row whose key reaches an object's prototype in the browser, passed because rows are otherwise not looked at."""
    next(iter(altair["datasets"].values()))[0]["__proto__"] = {"polluted": True}
    assert checked(altair) == vlspec.PROTOTYPE


def test_usermeta_is_dropped_everywhere(altair):
    """Bug class: vega-embed's options (an editor link, a loader, the eval it would need) set from the spec through `usermeta.embedOptions`."""
    altair["usermeta"] = {"embedOptions": {"actions": {"editor": True}, "editorUrl": "https://example.org/", "ast": False}}
    altair["layer"][0]["usermeta"] = {"embedOptions": {"loader": {"baseURL": "https://example.org/"}}}
    found = checked(altair)
    assert "usermeta" not in found and "usermeta" not in found["layer"][0]


def test_rows_and_views_are_bounded(altair):
    """Bug class: a spec of any number of rows or layers handed to the browser, which then stalls the Chat window."""
    name = next(iter(altair["datasets"]))
    altair["datasets"][name] = [{"posts": 1}] * vlspec.ROWS_MAX
    assert isinstance(checked(altair), dict)
    altair["datasets"]["more"] = [{"posts": 1}]
    assert checked(altair) == vlspec.TOO_MANY_ROWS
    split = {"layer": [{"data": {"values": [{"a": 1}] * 30000}, "mark": "point"}, {"data": {"values": [{"a": 1}] * 30000}, "mark": "point"}]}
    assert checked(split) == vlspec.TOO_MANY_ROWS
    views = {"hconcat": [{"mark": "point"}] * vlspec.VIEWS_MAX}
    assert isinstance(checked(views), dict)
    assert checked({"vconcat": [views, {"mark": "point"}]}) == vlspec.TOO_MANY_VIEWS
    assert checked({"repeat": {"row": ["a"] * 11, "column": ["b"] * 10}, "spec": {"mark": "point"}}) == vlspec.TOO_MANY_VIEWS


@pytest.mark.parametrize("payload, reason", [
    (b'{"mark": "point", "data": {"values": [{"a": NaN}]}}', vlspec.NOT_JSON),
    (b'{"mark": "point", "data": {"values": [{"a": Infinity}]}}', vlspec.NOT_JSON),
    (b'{"mark": "point", "data": {"values": [{"a": -Infinity}]}}', vlspec.NOT_JSON),
    (b'{"mark": "point", "data": {"values": [{"a": 1e400}]}}', vlspec.NOT_JSON),
    (b'{"mark": "point", "data": {"values": [{"a": 9007199254740993}]}}', vlspec.NOT_JSON),
    (b'{"mark": "point", "data": {"values": [{"a": ' + b"9" * 5000 + b"}]}}", vlspec.NOT_JSON),
    (b'{"mark": "point", "mark": "image"}', vlspec.NOT_JSON),
    (b'{"mark": "point"', vlspec.NOT_JSON),
    (b'{"mark": "point", "title": "\xff"}', vlspec.NOT_JSON),
    (b'{"mark": "point", "data": {"values": ' + b"[" * 70 + b"]" * 70 + b"}}", vlspec.TOO_DEEP),
    (b"[" * 200000 + b"]" * 200000, vlspec.TOO_DEEP),
    (b'{"mark": "point", "data": {"values": [' + b"[]," * 200001 + b"[]]}}", vlspec.TOO_MANY),
    (b'{"mark": "point", "title": "' + b"a" * vlspec.BYTES_MAX + b'"}', vlspec.TOO_LARGE),
    (b'["mark"]', vlspec.NOT_VEGA_LITE),
    (b'{"marks": [], "signals": [], "scales": []}', vlspec.NOT_VEGA_LITE),
])
def test_only_strict_bounded_json_of_vega_lite_passes(payload, reason):
    """Bug class: a spec the browser reads differently from the check (a repeated key: Python and the browser keep different ones), a number the browser cannot hold, nesting that exhausts the parser's recursion, a file of any size, or a Vega spec passed as a Vega-Lite one."""
    assert vlspec.check(payload) == (None, reason)


def test_brackets_inside_strings_do_not_count_as_nesting():
    """Bug class: a title full of brackets or escaped quotes refused as too deep, or a string that hides real nesting from the count."""
    title = '\\"' + "[" * 100 + "{" * 100
    payload = json.dumps({"mark": "point", "title": title}).encode()
    assert vlspec.shape(payload) == (1, 1) and json.loads(vlspec.check(payload)[0])["title"] == title
    assert vlspec.shape(b'{"a": "\\\\", "b": [[[]]]}') == (4, 4)
