"""The Chat window's data in reach (`GET /chat/reach`, `webui.reach`) and the model of a chat (the header's picker, the new-chat form's default, `hello`'s allow-list), through the whole application with the stand-in broker and the scripted stand-in of the runner (`webui_helpers.ScriptedRunner`).

The contracts pinned here, each test naming the bug class it catches: a database is listed with sources only when the chat attaches it, it runs and its read-only MCP server runs, each condition alone excludes it with its line; a source only under a database that holds data loaded from it; every failure confined to the block; the broker asked before anything else; the picker's model sent to the runner exactly; `hello`'s new fields checked with bounds.
"""

from __future__ import annotations

import html
import re

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import CHAT_ID, ORIGIN, db_status, form_token, log_in, refuse, source_list, source_size, source_status

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import assistant, chat, overview, queue, reach, words

MARK = "made-up-hostile-mark-5c1e"
HX = {"hx-request": "true"}
NOT_UNDERSTOOD = words.CHAT_CODE_SENTENCES["not_understood"]
HELLO = {"version": 1, "backend": "claude-code", "databases": ["starrocks", "mongo", "postgres"], "queue": False, "models": ["opus", "sonnet", "haiku"], "model_default": None}
DATABASES = ("starrocks", "mongo", "postgres")
VIEWS = {"sr_ingest": ("sr_ingest", ""), "mongo_ingest": ("mongo", "ingest_"), "postgres_ingest": ("postgres", "ingest_")}


@pytest.fixture
def app(make_app, scripted):
    return make_app(assistant=assistant.Assistant(scripted.path))


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


@pytest.fixture
def token(client):
    return form_token(client)


def post(client, path, token, **fields):
    return client.post(path, data={"form_token": token, **fields}, headers={"origin": ORIGIN})


def text_of(part):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", part)).split())


def status(running=DATABASES, mcp=DATABASES, mcp_running=DATABASES, read_only=DATABASES, configured=DATABASES):
    """A `db.status` answer: the databases of `configured`, running when in `running`; an MCP entry for each of `mcp`, running and read-only by the other lists."""
    return db_status(
        configured=list(configured),
        databases=[{"db": name, "port": "1", "running": name in running, "ingestion": {}} for name in configured],
        mcp=[{"db": name, "port": 1, "read_only": name in read_only, "running": name in mcp_running} for name in mcp],
    )


def loads(profile, count, failed=0):
    """A `source.status` ingestion view with one state file of `profile` that loaded `count` files."""
    db_type, prefix = VIEWS[profile]
    return {"db_type": db_type, "has_state_files": True, "entries": [{"label": f"{prefix}submissions", "count": count, "latest": None, "in_progress": None, "failed_count": failed, "last_updated": None}]}


def with_sources(broker, sources):
    """The broker's `source.list` and `source.status` for `sources`: {name: [(profile, loaded count)]}."""
    broker.answers["source.list"] = source_list(*({"name": name, "profiles": [profile for profile, _ in views]} for name, views in sources.items()))
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=[profile for profile, _ in sources[fields["name"]]], ingestion=[loads(profile, count) for profile, count in sources[fields["name"]]])


ROW = r'<span class="row[^"]*"><span class="dot[^"]*"></span>(.*?)</span>(?:\s*<ul class="reach-list">(.*?)</ul>)?'


def rows_of(part):
    """Each database's line as text: an out-of-reach one as drawn ("MongoDB: stopped"), one in reach as its title and its list's items, one per `<li>` ("MongoDB: voat (1.4 GB), 4chan (1.4 GB)")."""
    return [text_of(title) + (": " + ", ".join(text_of(item) for item in re.findall(r"<li>(.*?)</li>", items, re.DOTALL)) if items else "") for title, items in re.findall(ROW, part, re.DOTALL)]


def reach_rows(client):
    answer = client.get("/chat/reach", headers=HX)
    assert answer.status_code == 200
    return rows_of(answer.text), answer.text


def asked(broker, verb):
    return [request["fields"] for request in broker.requests if request["verb"] == verb]


# ── data in reach ───────────────────────────────────────────────────────────


def test_each_database_lists_the_sources_whose_data_it_holds(client, scripted, broker):
    """Bug class: a source listed under a database it never loaded into (or with no file loaded), or a database in reach without its sources."""
    scripted.answers["hello"] = HELLO
    broker.answers["db.status"] = status()
    with_sources(broker, {"reddit": [("sr_ingest", 40)], "voat": [("mongo_ingest", 12)], "4chan": [("mongo_ingest", 3), ("sr_ingest", 0)], "empty": [("sr_ingest", 0)], "fresh": [("postgres_ingest", 0)]})
    rows, part = reach_rows(client)
    assert rows == ["StarRocks: reddit (1.4 GB)", "MongoDB: voat (1.4 GB), 4chan (1.4 GB)", "PostgreSQL: no data loaded"]
    assert part.count('<span class="dot ok">') == 2 and 'id="chat-reach"' in part and "hx-get" not in part
    assert part.count('<ul class="reach-list">') == 2 and part.count("<li>") == 3


def test_a_source_under_two_databases_is_sized_once_and_shown_under_both(client, scripted, broker):
    """Bug class: one source's folders measured once per database it loaded into (each `source.size` walks every file of the source), or the two lines showing different sizes; a source with no data in reach measured at all."""
    scripted.answers["hello"] = HELLO
    broker.answers["db.status"] = status()
    with_sources(broker, {"reddit": [("sr_ingest", 40), ("mongo_ingest", 2)], "voat": [("mongo_ingest", 12)], "empty": [("sr_ingest", 0)]})
    broker.answers["source.size"] = lambda fields: source_size(fields["name"], total_bytes={"reddit": 999_950, "voat": 12_345_678}[fields["name"]], complete=fields["name"] == "reddit")
    rows, part = reach_rows(client)
    assert rows[:2] == ["StarRocks: reddit (1.0 MB)", "MongoDB: reddit (1.0 MB), voat (≥ 12.3 MB)"]
    assert [fields["name"] for fields in asked(broker, "source.size")] == ["reddit", "voat"]
    assert f'<span class="reach-size muted" title="{overview.SIZE_AT_LEAST}">(≥ 12.3 MB)</span>' in part and "./data/" not in part


@pytest.mark.parametrize("how", ["failed", "shape", "not found"])
def test_a_size_that_cannot_be_read_leaves_the_source_without_one_and_a_note(client, scripted, broker, how):
    """Bug class: a failed or malformed size read hiding the source (it is in reach, its loads were read), shown as "0 B", or raising out of the part."""
    scripted.answers["hello"] = HELLO
    broker.answers["db.status"] = status()
    with_sources(broker, {"reddit": [("sr_ingest", 40)], "voat": [("mongo_ingest", 12)]})
    bad = {"failed": broker_client.BrokerError("failed", MARK), "shape": source_size("reddit", total_bytes=True), "not found": source_size("reddit", found=False)}[how]
    broker.answers["source.size"] = lambda fields: bad if fields["name"] == "reddit" else source_size(fields["name"])
    rows, part = reach_rows(client)
    assert rows[:2] == ["StarRocks: reddit", "MongoDB: voat (1.4 GB)"] and MARK not in part
    assert "The size of 1 source could not be read." in part


CONDITIONS = [
    ("not attached", {"databases": ["mongo"]}, {}, "StarRocks: " + reach.NOT_ATTACHED),
    ("stopped", {}, {"running": ("mongo",)}, "StarRocks: stopped"),
    ("no MCP server", {}, {"mcp": ("mongo",)}, "StarRocks: no MCP server set up"),
    ("MCP down", {}, {"mcp_running": ("mongo",)}, "StarRocks: MCP server down"),
    ("MCP writes", {}, {"read_only": ("mongo",)}, "StarRocks: MCP server not read-only"),
    ("not set up", {}, {"configured": ("mongo",), "mcp": ("mongo",)}, "StarRocks: not set up"),
]


@pytest.mark.parametrize("name, hello, facts, line", CONDITIONS, ids=[c[0] for c in CONDITIONS])
def test_each_condition_alone_takes_a_database_out_of_reach_with_its_line(client, scripted, broker, name, hello, facts, line):
    """Bug class: the block claiming the assistant can query a database it cannot (not attached by the gateway, stopped, without a running read-only MCP server), or hiding why."""
    scripted.answers["hello"] = {**HELLO, "databases": ["starrocks", "mongo"], **hello}
    broker.answers["db.status"] = status(**{"configured": ("starrocks", "mongo"), "running": ("starrocks", "mongo"), "mcp": ("starrocks", "mongo"), "mcp_running": ("starrocks", "mongo"), "read_only": ("starrocks", "mongo"), **facts})
    with_sources(broker, {"reddit": [("sr_ingest", 40)], "voat": [("mongo_ingest", 12)]})
    rows, part = reach_rows(client)
    assert line in rows and "MongoDB: voat (1.4 GB)" in rows and "reddit" not in part
    assert [fields["name"] for fields in asked(broker, "source.status")] == ["voat"]


def test_no_source_is_asked_when_no_database_is_in_reach(client, scripted, broker):
    scripted.answers["hello"] = {**HELLO, "databases": ["mongo"]}
    broker.answers["db.status"] = status(running=())
    assert reach_rows(client)[0] == ["MongoDB: stopped", "StarRocks: " + reach.NOT_ATTACHED, "PostgreSQL: " + reach.NOT_ATTACHED]
    assert asked(broker, "source.list") == [] and asked(broker, "source.status") == []


def test_the_broker_is_asked_before_anything_else(client, scripted, broker, clock):
    """Bug class: the runner or the broker's reads asked for a session the broker has ended or could not confirm."""
    scripted.answers["hello"] = HELLO
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    broker.requests.clear()
    broker.answers["audit.list"] = broker_client.BrokerUnreachable(broker_client.NO_SOCKET)
    rows, _ = reach_rows(client)
    assert rows == [f"Not known: the broker did not answer: {words.REASON_SENTENCES[broker_client.NO_SOCKET]}"]
    assert [request["verb"] for request in broker.requests] == ["audit.list"] and scripted.requests == []
    broker.answers["audit.list"] = broker_client.BrokerError("session_ended", "x")
    ended = client.get("/chat/reach", headers=HX)
    assert ended.status_code == 204 and ended.headers["hx-redirect"] == "/login?why=ended"
    assert [request["verb"] for request in broker.requests] == ["audit.list", "audit.list"] and scripted.requests == []


def test_a_session_the_broker_ends_midway_ends_the_web_session(client, scripted, broker):
    scripted.answers["hello"] = HELLO
    broker.answers["db.status"] = broker_client.BrokerError("unauthorized", "x")
    ended = client.get("/chat/reach", headers=HX)
    assert ended.status_code == 204 and ended.headers["hx-redirect"].startswith("/login")


@pytest.mark.parametrize("where", ["hello", "db.status", "db.status shape", "source.list", "one source"])
def test_a_failure_is_confined_to_the_block(client, scripted, broker, where):
    """Bug class: one failing read taking the part (or the page) down, or the part claiming data in reach it could not read."""
    scripted.answers["hello"] = refuse("internal") if where == "hello" else HELLO
    broker.answers["db.status"] = broker_client.BrokerError("failed", MARK) if where == "db.status" else {"data": MARK} if where == "db.status shape" else status()
    with_sources(broker, {"reddit": [("sr_ingest", 40)], "voat": [("mongo_ingest", 12)]})
    if where == "source.list":
        broker.answers["source.list"] = broker_client.BrokerError("failed", MARK)
    if where == "one source":
        good = broker.answers["source.status"]
        broker.answers["source.status"] = lambda fields: (_ for _ in ()).throw(broker_client.BrokerError("failed", MARK)) if fields["name"] == "reddit" else good(fields)
    rows, part = reach_rows(client)
    assert MARK not in part and "Traceback" not in part
    if where in ("hello", "db.status", "db.status shape"):
        assert len(rows) == 1 and rows[0].startswith("Not known:")
    elif where == "source.list":
        assert rows[:3] == ["StarRocks: " + reach.LOADS_NOT_READ, "MongoDB: " + reach.LOADS_NOT_READ, "PostgreSQL: " + reach.LOADS_NOT_READ] and "The sources could not be read" in part
    else:
        assert rows[:2] == ["StarRocks: " + reach.LOADS_NOT_READ, "MongoDB: voat (1.4 GB)"] and "The loads of 1 source could not be read." in part


def test_a_source_name_that_is_not_plain_is_never_asked_or_shown(client, scripted, broker):
    scripted.answers["hello"] = HELLO
    broker.answers["db.status"] = status()
    hostile = "<b>" + MARK
    broker.answers["source.list"] = source_list({"name": hostile, "profiles": ["sr_ingest"]}, {"name": "reddit", "profiles": ["sr_ingest"]})
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=["sr_ingest"], ingestion=[loads("sr_ingest", 5)])
    rows, part = reach_rows(client)
    assert rows[0] == "StarRocks: reddit (1.4 GB)" and MARK not in part and [fields["name"] for fields in asked(broker, "source.status")] == ["reddit"] and [fields["name"] for fields in asked(broker, "source.size")] == ["reddit"]


def test_the_reads_are_bounded_in_sources_and_time():
    """Bug class: a deployment with hundreds of sources (or a slow broker) holding the part's worker without bound."""
    now = [0.0]

    def clock():
        return now[0]

    calls = []

    def ask(verb, fields):
        calls.append(verb)
        if verb == "db.status":
            return "ok", status()
        if verb == "source.list":
            return "ok", source_list(*({"name": f"s{index}", "profiles": ["sr_ingest"]} for index in range(60)))
        # A status read takes half a second, a size read one: the time runs out between the 14th source's status and its size.
        if verb == "source.size":
            now[0] += 1
            return "ok", source_size(fields["name"])
        now[0] += 0.5
        return "ok", source_status(fields["name"], profiles=["sr_ingest"], ingestion=[loads("sr_ingest", 1)])

    model = reach.read_reach(lambda: None, lambda: ("ok", HELLO), ask, clock=clock)
    assert reach.LIVE_TOTAL_SECONDS == 20 and calls.count("source.status") == 14 and calls.count("source.size") == 13
    assert f"Only the first {reach.LIVE_SOURCES_MAX} of 60 sources were read." in model["notes"]
    assert f"The loads of {reach.LIVE_SOURCES_MAX - 14} sources were not read: the time for them ran out." in model["notes"]
    assert "The size of 1 source was not read: the time for it ran out." in model["notes"]
    shown = model["rows"][0]["sources"]
    assert [source["name"] for source in shown[:2]] == ["s0", "s1"] and len(shown) == 14
    assert shown[0]["size"]["text"] == "1.4 GB" and shown[13] == {"name": "s13", "size": None}


def test_the_page_draws_a_placeholder_that_asks_the_part_once(client, scripted):
    scripted.answers["hello"] = HELLO
    page = client.get("/chat").text
    assert '<div class="col" id="chat-reach" hx-get="/chat/reach" hx-trigger="load" hx-swap="outerHTML">' in page
    assert "StarRocks: checking" in text_of(page)


READ_VERBS = ("db.status", "source.list", "source.status", "source.size")
PLACEHOLDER = '<div class="col" id="chat-reach" hx-get="/chat/reach" hx-trigger="load" hx-swap="outerHTML">'


def reads(broker):
    return [request["verb"] for request in broker.requests if request["verb"] in READ_VERBS]


def block_of(page):
    """The opening tag of the block and its rows' text."""
    found = re.search(r'(<div class="col" id="chat-reach"[^>]*>)(.*?)</div>', page, re.DOTALL)
    return found.group(1), text_of(found.group(2))


@pytest.fixture
def kept(client, scripted, broker):
    """A login that has read the block once: StarRocks holds reddit."""
    scripted.answers["hello"] = HELLO
    broker.answers["db.status"] = status()
    with_sources(broker, {"reddit": [("sr_ingest", 40)]})
    assert reach_rows(client)[0][0] == "StarRocks: reddit (1.4 GB)"
    return reads(broker)


def test_a_kept_read_is_drawn_at_once_and_not_asked_again_within_a_minute(client, broker, clock, kept):
    """Bug class: every Chat page (opening a chat, sending, switching) re-reading the whole block (up to ~53 broker calls) and showing "checking…" although this login read it moments ago."""
    clock.now += chat.REACH_FRESH_SECONDS - 1
    for page in (client.get("/chat").text, client.get(f"/chat/{CHAT_ID}").text):
        tag, rows = block_of(page)
        assert tag == '<div class="col" id="chat-reach">' and rows.startswith("StarRocks reddit (1.4 GB)") and "checking" not in text_of(page)
    assert reads(broker) == kept


def test_the_data_in_reach_is_a_box_that_opens_and_closes_at_the_bottom_of_the_left_column(client, kept):
    """Bug class: the block outside its box (it would not collapse, or the docking rules miss it), the box closed by default, the queue's line left out of it, or the sources joined on one line again."""
    page = client.get("/chat").text
    left = page.split('<aside class="left"', 1)[1].split("</aside>", 1)[0]
    box = re.search(r'<details class="reach-box" open>\s*<summary>Data in reach</summary>(.*?)</details>', left, re.DOTALL)
    assert box and left.rstrip().endswith("</details>") and "<h4>Data in reach</h4>" not in page
    assert '<div class="col" id="chat-reach">' in box[1] and '<ul class="reach-list">' in box[1] and "Queue · " in text_of(box[1])
    assert rows_of(box[1])[0] == "StarRocks: reddit (1.4 GB)"


def test_a_stale_kept_read_is_drawn_and_refreshed_in_place(client, broker, clock, kept):
    """Bug class: a kept read shown for the whole login however old (a database started or stopped never showing), or a stale one hidden behind "checking…"."""
    clock.now += chat.REACH_FRESH_SECONDS + 1
    tag, rows = block_of(client.get("/chat").text)
    assert tag == PLACEHOLDER and rows.startswith("StarRocks reddit (1.4 GB)")
    assert reads(broker) == kept
    reach_rows(client)
    assert block_of(client.get("/chat").text)[0] == '<div class="col" id="chat-reach">'


def test_a_whole_block_failure_is_not_kept(client, scripted, broker, clock, kept):
    """Bug class: a read the broker could not answer kept for the login (the block saying "Not known" for a minute after the broker is back), or an older read drawn as fact after the broker said it could not be read."""
    clock.now += chat.REACH_FRESH_SECONDS + 1
    broker.answers["audit.list"] = broker_client.BrokerUnreachable(broker_client.NO_SOCKET)
    assert reach_rows(client)[0][0].startswith("Not known: the broker did not answer")
    broker.answers["audit.list"] = {"data": []}
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    tag, rows = block_of(client.get("/chat").text)
    assert tag == PLACEHOLDER and rows.startswith("StarRocks: checking")


# ── the model of a chat ─────────────────────────────────────────────────────


def sent(scripted, op):
    return [request for request in scripted.requests if request.get("op") == op]


def test_the_picker_shows_the_allow_list_with_the_chats_model_chosen(client, scripted):
    scripted.answers["hello"] = HELLO
    scripted.chats = [{**scripted.chats[0], "model": "haiku"}]
    page = client.get(f"/chat/{CHAT_ID}").text
    picker = re.search(r'<select id="chat-model" name="model" form="chat-send"[^>]*>(.*?)</select>', page, re.DOTALL)
    assert picker and 'id="chat-send"' in page
    assert re.findall(r'<option value="([^"]*)"( selected)?>', picker.group(1)) == [("", ""), ("opus", ""), ("sonnet", ""), ("haiku", " selected")]


@pytest.mark.parametrize("fields, expected", [({"model": "opus"}, {"model": "opus"}), ({"model": ""}, {"model": None}), ({}, {})])
def test_the_picker_posts_the_model_and_the_runner_receives_exactly_it(client, scripted, token, fields, expected):
    """Bug class: the chosen model lost on the way (the turn runs on another), "backend default" sent as a name, or a send without the picker changing the chat's model."""
    scripted.answers["hello"] = HELLO
    assert post(client, f"/chat/{CHAT_ID}/send", token, text="a question", **fields).status_code == 303
    assert sent(scripted, "chat.send") == [{"op": "chat.send", "chat": CHAT_ID, "text": "a question", **expected}]


@pytest.mark.parametrize("value", ["a b", "x" * 201, "é", "opus\n"])
def test_a_model_field_outside_the_rule_is_refused_and_nothing_is_sent(client, scripted, token, value):
    assert post(client, f"/chat/{CHAT_ID}/send", token, text="q", model=value).status_code == 400
    assert post(client, "/chat/new", token, model=value).status_code == 400
    assert sent(scripted, "chat.send") == [] and sent(scripted, "chat.new") == []


def test_a_refused_model_says_so(client, scripted, token):
    scripted.answers["chat.send"] = refuse("model_not_allowed")
    answer = post(client, f"/chat/{CHAT_ID}/send", token, text="q", model="opus")
    assert answer.status_code == 409 and words.CHAT_CODE_SENTENCES["model_not_allowed"] in html.unescape(answer.text)


@pytest.mark.parametrize("fields, expected", [({"model": "haiku"}, {"model": "haiku"}), ({"model": ""}, {}), ({}, {})])
def test_a_new_chat_sends_the_default_model_of_its_form(client, scripted, token, fields, expected):
    """Bug class: the browser's default model for new chats not reaching the runner, or an empty field sent as a model."""
    assert post(client, "/chat/new", token, **fields).status_code == 303
    assert sent(scripted, "chat.new") == [{"op": "chat.new", **expected}]
    assert post(client, "/chat/new", token, model="opus", other="x").status_code == 400


def test_the_new_chat_form_carries_the_offered_models_for_app_js(client, scripted):
    scripted.answers["hello"] = HELLO
    page = client.get("/chat").text
    assert page.count('<input type="hidden" class="default-model" name="model" value="" data-models="opus sonnet haiku">') == 2
    scripted.answers["hello"] = {**HELLO, "models": ["vendor/m-1"], "model_default": "vendor/m-1"}
    assert "default-model" not in client.get("/chat").text


def test_an_openai_compatible_back_end_shows_its_one_model_not_choosable(client, scripted):
    scripted.answers["hello"] = {**HELLO, "backend": "openai-compatible", "models": ["vendor/m-1"], "model_default": "vendor/m-1"}
    page = client.get(f"/chat/{CHAT_ID}").text
    assert "<select" not in page.split('<section class="mid"')[1].split("</section>")[0] and '<span class="tag">vendor/m-1</span>' in page


HOSTILE_HELLOS = [
    {**HELLO, "models": MARK},
    {**HELLO, "models": ["opus"] * 2},
    {**HELLO, "models": [f"m{index}" for index in range(assistant.MODELS_MAX + 1)]},
    {**HELLO, "models": ["a b"]},
    {**HELLO, "models": ["x" * 201]},
    {**HELLO, "models": ["é"]},
    {**HELLO, "models": [5]},
    {**HELLO, "model_default": "gpt"},
    {**HELLO, "model_default": 5},
    {key: value for key, value in HELLO.items() if key != "model_default"},
    {key: value for key, value in HELLO.items() if key != "models"},
    {**HELLO, "extra": MARK},
]


@pytest.mark.parametrize("hello", HOSTILE_HELLOS)
def test_a_hello_with_hostile_model_fields_is_not_understood(client, scripted, hello):
    """Bug class: the runner (which code in the sandbox can replace) putting unbounded or invisible names into the picker, or a default the list does not hold."""
    scripted.answers["hello"] = hello
    with pytest.raises(assistant.NotUnderstood):
        assistant.Assistant(scripted.path).hello()
    page = client.get(f"/chat/{CHAT_ID}").text
    assert NOT_UNDERSTOOD in html.unescape(page) and MARK not in page and 'id="chat-model"' not in page


@pytest.mark.parametrize("model", [5, "a b", "x" * 201, ["opus"]])
def test_a_chat_list_with_a_hostile_model_is_not_understood(scripted, model):
    scripted.chats = [{**scripted.chats[0], "model": model}]
    with pytest.raises(assistant.NotUnderstood):
        assistant.Assistant(scripted.path).chats()


def test_the_model_sentence_covers_the_runners_new_code():
    from social_data_pipeline.chat import runner

    assert "model_not_allowed" in runner.MESSAGES and "model_not_allowed" in words.CHAT_CODE_SENTENCES and chat.model_field({"model": "opus"}) == (True, "opus")
