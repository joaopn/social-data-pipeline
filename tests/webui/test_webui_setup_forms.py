"""Tests for the setup verbs' kinds of the web UI's table (`webui.actions`: `path`, `port`, `text`, `list`, `map`, `record`), the form scheme that posts them, and the macros of `_forms.html` that draw them.

The contracts pinned here, each test naming the bug class it catches:

- The checks of the new kinds (`check_fields`) take what the registry's types take and refuse the rest, at any depth.
- The form scheme (`parse_form`): bracketed names (`targets[2][name]`, `fields[comments][1]`, `targets[-remove][2]`) turn into the JSON the broker expects; empty rows and removed rows are dropped and the order kept; every name or shape outside the scheme (a gap, a leading zero, an index past the list's size, a key the record does not have, a value where a branch belongs and the reverse, a bracketed name of a plain input) is refused with the top-level input's name.
- `call` sends nested secrets as text, and the pending page shows them as dots.
- The macros: what a browser posts from a drawn form parses back into the value it was drawn with; a secret is never drawn; no inline style, script or event attribute.

The pure tests need no FastAPI; the drawing tests skip without Jinja2.
"""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path

import pytest

from social_data_pipeline.webui import actions

Input = actions.Input
SECRET = "made-up-setup-secret-41c"

TARGET = Input("record", "Target", fields={
    "name": Input("name", "Name", required=True),
    "database": Input("enum", "Database", choices=("postgres", "starrocks")),
    "timeout_hours": Input("int", "Timeout (hours)", lo=1, hi=72),
    "export": Input("bool", "Export"),
})
SETUP = actions.Action("test.setup", "step_up", "A setup made for the tests", {
    "port": Input("port", "Port", required=True),
    "results": Input("path", "Results folder"),
    "pgtune": Input("text", "PGTune output", max_bytes=200, multiline=True),
    "label": Input("text", "One line", max_bytes=20),
    "targets": Input("list", "Targets", item=TARGET, max_items=3),
    "folders": Input("list", "Extra folders", item=Input("path", "Folder"), max_items=4, unique=True),
    "fields": Input("map", "Fields", key=Input("name", "Data type"), item=Input("list", "Fields", item=Input("name", "Field"), max_items=4), max_items=3),
    "users": Input("list", "Users", item=Input("record", "User", fields={"user": Input("name", "User", required=True), "password": Input("secret", "Password")}), max_items=2),
    "limits": Input("record", "Limits", fields={"slots": Input("int", "Slots", lo=1, hi=8), "keep_days": Input("int", "Days kept", lo=1, hi=365)}),
}, command=lambda fields: "python sdp.py db setup-jobs")


def parse(form):
    return actions.parse_form(SETUP, form)


def refused(form):
    with pytest.raises(actions.Refused) as exc:
        parse(form)
    return exc.value.field


# ── the checks of the new kinds ─────────────────────────────────────────────


@pytest.mark.parametrize("name, value", [
    ("port", 1024), ("port", 65535),
    ("results", "/mnt/x y/café"), ("results", "data/jobs"),
    ("pgtune", "a\n\tb\n"), ("pgtune", ""), ("pgtune", "é" * 100),
    ("targets", []), ("targets", [{"name": "w"}, {"name": "x", "database": "postgres", "timeout_hours": 72, "export": True}]),
    ("folders", ["/a", "/b"]),
    ("fields", {"submissions": ["id", "title"]}),
    ("limits", {"slots": 2}),
])
def test_the_new_kinds_take_what_the_registry_takes(name, value):
    """Bug class: a check stricter than the registry's type, so a value the terminal and the broker take is refused by the page."""
    actions.check_fields(SETUP, {"port": 8080, name: value})


@pytest.mark.parametrize("name, value", [
    ("port", 1023), ("port", 65536), ("port", True), ("port", "8080"),
    ("results", ""), ("results", "/x\n"), ("results", "/x\ty"), ("results", "/x\x00"), ("results", "/x "), ("results", "/" + "a" * 4096), ("results", "/\ud800"),
    ("pgtune", "a\rb"), ("pgtune", "a\x1bb"), ("pgtune", "é" * 101), ("label", "a\nb"),
    ("targets", [{"database": "postgres"}]), ("targets", [{"name": "w", "extra": 1}]), ("targets", [{"name": "w"}] * 4), ("targets", [{"name": "../w"}]), ("targets", {"name": "w"}), ("targets", [{"name": "w", "database": "mongo"}]),
    ("folders", ["/a", "/a"]), ("folders", "/a"),
    ("fields", {"a b": []}), ("fields", {"a": ["x"] * 5}), ("fields", {"a": [], "b": [], "c": [], "d": []}),
    ("limits", {"slots": 9}), ("limits", {"other": 1}),
    ("users", [{"user": "ro", "password": SECRET}]),
])
def test_the_new_kinds_refuse_what_the_registry_refuses(name, value):
    """Bug class: a page that keeps a pending action the broker will refuse after the operator confirmed it with the management password (a privileged port, a path with a line break, a list too long, a record with a key the registry does not list, a secret not wrapped in Hidden)."""
    with pytest.raises(actions.Refused) as exc:
        actions.check_fields(SETUP, {"port": 8080, name: value})
    assert exc.value.field == name


@pytest.mark.parametrize("build", [
    lambda: Input("path", "P", max_bytes=4097),
    lambda: Input("text", "T"),
    lambda: Input("list", "L", item=Input("name", "N")),
    lambda: Input("list", "L", item=Input("name", "N"), max_items=1025),
    lambda: Input("list", "L", item=Input("secret", "S"), max_items=2, unique=True),
    lambda: Input("map", "M", key=Input("path", "K"), item=Input("name", "N"), max_items=2),
    lambda: Input("record", "R", fields={}),
    lambda: Input("record", "R", fields={"Bad": Input("name", "N")}),
])
def test_a_malformed_setup_input_is_refused_when_the_table_is_built(build):
    """Bug class: a typo in the table that declares an unbounded or unquotable shape the registry would refuse to build."""
    with pytest.raises(ValueError):
        build()


# ── the form scheme ─────────────────────────────────────────────────────────


def test_the_scheme_turns_bracketed_names_into_the_json_the_broker_expects():
    """Bug class: a form whose nested values reach the broker in another shape than its types (a row as a flat key, a number as text, a path not normalized, a textarea's CRLF kept), or whose rows come back in another order."""
    fields = parse({
        "port": "8080",
        "results": "data/./jobs/",
        "pgtune": "a\r\nb\rc",
        "targets[0][name]": "warehouse", "targets[0][database]": "starrocks", "targets[0][timeout_hours]": "4", "targets[0][export]": "yes",
        "targets[1][name]": "", "targets[1][database]": "", "targets[1][timeout_hours]": "",
        "targets[2][name]": "pg",
        "folders[0]": "/b", "folders[1]": "", "folders[2]": "/a",
        "fields[submissions][0]": "id", "fields[submissions][1]": "title", "fields[comments][0]": "",
        "users[0][user]": "ro", "users[0][password]": SECRET,
        "limits[slots]": "2", "limits[keep_days]": "",
    })
    assert actions.revealed(fields) == {
        "port": 8080,
        "results": "data/jobs",
        "pgtune": "a\nb\nc",
        "targets": [{"name": "warehouse", "database": "starrocks", "timeout_hours": 4, "export": True}, {"name": "pg"}],
        "folders": ["/b", "/a"],
        "fields": {"submissions": ["id", "title"]},
        "users": [{"user": "ro", "password": SECRET}],
        "limits": {"slots": 2},
    }
    assert isinstance(fields["users"][0]["password"], actions.Hidden)


def test_removed_rows_and_empty_containers_are_dropped():
    """Bug class: a ticked Remove box ignored (the row the operator removed is written), or an empty list sent as [] where nothing was typed (an action reads [] as "remove them all")."""
    fields = parse({"port": "8080", "targets[0][name]": "a", "targets[1][name]": "b", "targets[2][name]": "c", "targets[-remove][1]": "yes", "folders[0]": "", "fields[comments][0]": ""})
    assert fields == {"port": 8080, "targets": [{"name": "a"}, {"name": "c"}]}
    assert parse({"port": "8080", "targets[0][name]": "a", "targets[-remove][0]": "yes"}) == {"port": 8080}


@pytest.mark.parametrize("form, field", [
    ({"targets[1][name]": "x"}, "targets"),
    ({"targets[0][name]": "x", "targets[2][name]": "y"}, "targets"),
    ({"targets[00][name]": "x"}, "targets"),
    ({"targets[-1][name]": "x"}, "targets"),
    ({"targets[+0][name]": "x"}, "targets"),
    ({"targets[0][name]": "a", "targets[1][name]": "b", "targets[2][name]": "c", "targets[3][name]": "d"}, "targets"),
    ({"targets[0][zone]": "x"}, "targets"),
    ({"targets[0]": "x"}, "targets"),
    ({"targets[0][name][x]": "y"}, "targets"),
    ({"targets[0][name]": "a", "targets[-remove][1]": "yes"}, "targets"),
    ({"targets[0][name]": "a", "targets[-remove][0]": "on"}, "targets"),
    ({"targets[0][name]": "a", "targets[-remove]": "yes"}, "targets"),
    ({"targets[0][export]": "true"}, "targets"),
    ({"targets[0][name]": "a", "targets[0][timeout_hours]": "4.5"}, "targets"),
    ({"targets[0][name]": "a", "targets[0][timeout_hours]": "04"}, "targets"),
    ({"targets": "x"}, "targets"),
    ({"fields[a b][0]": "x"}, "fields"),
    ({"fields[-remove][0]": "yes"}, "fields"),
    ({"fields[submissions]": "x"}, "fields"),
    ({"fields[a][0]": "x", "fields[b][0]": "x", "fields[c][0]": "x", "fields[d][0]": "x"}, "fields"),
    ({"limits[other]": "1"}, "limits"),
    ({"folders[0]": "/a", "folders[1]": "/a/."}, "folders"),
    ({"port": "80"}, "port"),
    ({"port": "-8080"}, "port"),
    ({"port": "8080 "}, "port"),
    ({"port": "٨٠٨٠"}, "port"),
    ({"results": "/x\ny"}, "results"),
    ({"users[0][password]": SECRET}, "users"),
])
def test_names_and_shapes_outside_the_scheme_are_refused(form, field):
    """Bug class: a parser that guesses at a hostile form (a gap read as a shorter list, `00` and `0` as one row, a row past the list's size, an unknown key passed through, a value and a branch at one place, a Unicode digit read as a number), so that the broker receives something no form of the page draws; the refusal names the top-level input."""
    assert refused({"port": "8080", **form}) == field


@pytest.mark.parametrize("name", ["targets[0", "targets]0[", "targets[0]x", "targets[]", "Targets[0][name]", "port[0]", "made_up[0]", "targets[0][name][a][b][c][d]", "targets[" + "9" * 65 + "]"])
def test_a_name_that_is_not_of_the_scheme_is_refused(name):
    """Bug class: a bracketed name of a plain input, an unknown input or a malformed bracket accepted as something else."""
    assert refused({"port": "8080", name: "x"}) in (None, "port", "targets")


def test_the_parser_is_bounded_by_the_guards_field_count():
    """Bug class: a parser whose work grows with something other than the number of posted fields, which the guard caps at 64: one deep name costs at most five segments."""
    assert actions.FORM_NAME.fullmatch("targets" + "[0]" * 5) is not None
    assert actions.FORM_NAME.fullmatch("targets" + "[0]" * 6) is None
    from social_data_pipeline.webui import guard

    assert guard.FORM_FIELDS_MAX == 64


def test_form_name_makes_the_schemes_names():
    assert actions.form_name("targets", 2, "name") == "targets[2][name]"
    assert actions.form_name("targets", actions.REMOVE_SEGMENT, 0) == "targets[-remove][0]"
    assert actions.form_name("port") == "port"


# ── sending and showing ─────────────────────────────────────────────────────


class RecordingClient:
    def __init__(self):
        self.sent = []

    def call(self, verb, fields=None, step_up=None):
        self.sent.append((verb, fields, step_up))
        return {}


def test_call_sends_nested_secrets_as_text_and_the_pending_page_shows_dots(monkeypatch):
    """Bug class: a nested secret sent still wrapped (the broker refuses it, or a repr reaches the wire), or shown in clear on the pending page."""
    monkeypatch.setattr(actions, "TABLE", {**actions.TABLE, "test.setup": SETUP})
    fields = parse({"port": "8080", "users[0][user]": "ro", "users[0][password]": SECRET, "targets[0][name]": "w", "targets[0][export]": "yes"})
    made = RecordingClient()
    actions.call(made, "test.setup", fields, "management-pw")
    assert made.sent == [("test.setup", {"port": 8080, "users": [{"user": "ro", "password": SECRET}], "targets": [{"name": "w", "export": True}]}, "management-pw")]
    shown = dict(actions.shown_inputs(SETUP, fields))
    assert SECRET not in repr(shown) and SECRET not in repr(fields)
    assert shown["Users"] == "User: ro; Password: " + actions.SECRET_DOTS
    assert shown["Targets"] == "Name: w; Export: yes"


# ── the macros ──────────────────────────────────────────────────────────────


class Posted(HTMLParser):
    """What a browser posts from a drawn form, as the guard hands it on: text, number, password and hidden boxes with their value, a ticked box, a textarea's text (its first newline dropped as the HTML parser does), the selected option of a select (else its first)."""

    def __init__(self):
        super().__init__()
        self.form = {}
        self._textarea = None
        self._select = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "input":
            kind = attrs.get("type", "text")
            if kind == "checkbox":
                if "checked" in attrs:
                    self.form[attrs["name"]] = attrs.get("value", "on")
            else:
                self.form[attrs["name"]] = attrs.get("value", "")
        elif tag == "textarea":
            self._textarea = (attrs["name"], [])
        elif tag == "select":
            self._select = (attrs["name"], [])
        elif tag == "option" and self._select is not None:
            self._select[1].append((attrs.get("value", ""), "selected" in attrs))

    def handle_data(self, data):
        if self._textarea is not None:
            self._textarea[1].append(data)

    def handle_endtag(self, tag):
        if tag == "textarea" and self._textarea is not None:
            name, parts = self._textarea
            text = "".join(parts)
            self.form[name] = text[1:] if text.startswith("\n") else text
            self._textarea = None
        elif tag == "select" and self._select is not None:
            name, options = self._select
            chosen = [value for value, selected in options if selected]
            self.form[name] = chosen[0] if chosen else options[0][0]
            self._select = None


@pytest.fixture
def draw():
    jinja2 = pytest.importorskip("jinja2")
    templates = Path(actions.__file__).resolve().parent / "templates"
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(str(templates)), autoescape=True, undefined=jinja2.StrictUndefined)
    page = environment.from_string('{% import "_forms.html" as forms %}{% for name, spec in action.inputs.items() %}{{ forms.field(name, spec, values.get(name), **options.get(name, {})) }}{% endfor %}')

    def render(values, options=None):
        return page.render(action=SETUP, values=values, options=options or {})

    return render


def posted(text):
    parser = Posted()
    parser.feed(text)
    parser.close()
    return parser.form


VALUES = {
    "port": 8080,
    "results": "/mnt/bulk/jobs",
    "pgtune": "\n# first line empty\nshared_buffers = 4GB\n",
    "label": "one <b>line</b>",
    "targets": [{"name": "warehouse", "database": "starrocks", "timeout_hours": 4, "export": True}, {"name": "pg", "database": "postgres"}],
    "folders": ["/a", "/b"],
    "fields": {"submissions": ["id", "title"], "comments": ["body"]},
    "users": [{"user": "ro", "password": actions.Hidden(SECRET)}],
    "limits": {"slots": 2, "keep_days": 30},
}


def test_a_drawn_form_posts_back_the_values_it_was_drawn_with(draw):
    """Bug class: a macro whose names are not the scheme's (the parser refuses the page's own form), a value drawn in a box under another name, a textarea that loses its first newline, a select that does not mark its value, a blank row that posts something."""
    text = draw(VALUES)
    form = posted(text)
    expected = {key: value for key, value in VALUES.items() if key != "users"}
    expected["users"] = [{"user": "ro"}]
    assert actions.revealed(parse(form)) == expected
    assert "targets[2][name]" in form and "targets[3][name]" not in form, "one blank row, up to max_items"
    assert form["targets[2][name]"] == "" and "targets[-remove][2]" not in text


def test_a_secret_is_never_drawn_and_hostile_values_are_escaped(draw):
    """Bug class: a secret written into the page's markup (a value attribute of a password box), or a stored value that breaks out of its attribute."""
    hostile = '"><script>x</script>'
    text = draw({**VALUES, "results": hostile, "targets": [{"name": hostile}]})
    assert SECRET not in text
    assert hostile not in text and "<script>" not in text


def test_every_macro_draws_without_inline_code(draw):
    """Bug class: a macro that draws a style attribute, an inline script or an event handler, which the content policy blocks (or tempts a loosening of it)."""
    pytest.importorskip("fastapi")
    from test_webui_pages import inline_code

    for values in (VALUES, {}):
        text = draw(values, {"targets": {}, "pgtune": {"hint": "Paste PGTune's output."}})
        assert inline_code(text) == []
        assert "<fieldset" in text and "<textarea" in text and 'type="number"' in text and 'class="mono"' in text


def test_a_map_draws_the_keys_the_page_names(draw):
    """Bug class: a map of fixed keys (a source's data types) drawn without the keys it has no value for yet, so the operator cannot fill them."""
    jinja2 = pytest.importorskip("jinja2")
    templates = Path(actions.__file__).resolve().parent / "templates"
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(str(templates)), autoescape=True, undefined=jinja2.StrictUndefined)
    text = environment.from_string('{% import "_forms.html" as forms %}{{ forms.map_input("fields", spec, {"comments": ["body"]}, keys=["submissions"]) }}').render(spec=SETUP.inputs["fields"])
    form = posted(text)
    assert form["fields[submissions][0]"] == "" and form["fields[comments][0]"] == "body"
    assert text.index("submissions") < text.index("comments")


def test_a_list_draws_more_rows_only_up_to_its_size_and_offers_the_reload(draw):
    """Bug class: a list drawn with more rows than the broker takes (a full form the parser then refuses), or with no way to add rows without script."""
    jinja2 = pytest.importorskip("jinja2")
    templates = Path(actions.__file__).resolve().parent / "templates"
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(str(templates)), autoescape=True, undefined=jinja2.StrictUndefined)
    page = environment.from_string('{% import "_forms.html" as forms %}{{ forms.list_input("folders", spec, values, blank=blank, more_href="/setup?rows=3") }}')
    spec = SETUP.inputs["folders"]
    few = page.render(spec=spec, values=["/a"], blank=1)
    assert sorted(name for name in posted(few) if name.startswith("folders[") and "-remove" not in name) == ["folders[0]", "folders[1]"]
    assert 'href="/setup?rows=3"' in few and "Remove" in few
    full = page.render(spec=spec, values=["/a"], blank=9)
    assert [name for name in posted(full) if "-remove" not in name] == ["folders[0]", "folders[1]", "folders[2]", "folders[3]"]
    assert "More rows" not in full
