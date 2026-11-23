"""The export of a whole chat as one JSON document, for `GET /chat/<id>/export` (`chat.install`).

The document, written as it is read and sent in pieces (never built whole): `{"format": 1, "exported_at", "chat": {"id", "title", "model", "backend", "created"}, "events": [every event of `chat.events` from the start, in order], "backend_transcript": [each line of the back end's own transcript (`chat.transcript`), parsed as JSON, or the line as a string when it is not a JSON object] or null (the back end keeps none, the chat has none yet, or it was not read), "stopped": null or the sentence that says where and why the export stopped}`. Bounded: at most `events_max` events, `bytes_max` bytes in all (the closing part kept within a reserve, so the document stays valid JSON whenever it stops), a total time limit looked at before every call of the runner, and one line of the transcript at most LINE_MAX_BYTES. Everything the runner sends is hostile: an event and a line are only ever written into the document by `json.dumps` (a lone surrogate escaped, a number JSON cannot hold replaced), never into markup, and the document is sent as an attachment under `Content-Security-Policy: sandbox`.
"""

import datetime
import json

import anyio
from starlette.responses import Response

from social_data_pipeline.control import client, framing
from social_data_pipeline.webui import assistant, words

FORMAT = 1
EVENTS_PAGE = 200
# The longest line of the transcript held while it is read; the room kept for the document's closing part; the bytes gathered before they are sent.
LINE_MAX_BYTES = 8 << 20
TAIL_RESERVE = 4096
SEND_BYTES = 64 * 1024
UNWRITABLE = {"export_note": "this item could not be written as JSON"}
STOPPED_EVENTS = "The export holds at most {count:,} events: the chat's later events are left out, and so is the back end's transcript."
STOPPED_BYTES = "The export reached {size}, the most one export holds: what follows is left out."
STOPPED_TIME = "The export took longer than {minutes:g} minutes, its time limit: what follows is left out."
STOPPED_LINE = "A line of the back end's transcript is longer than 8 MiB: it and what follows are left out."
EVENTS_FAILED = "The chat's events could not all be read, and what follows is left out: {sentence}"
TRANSCRIPT_FAILED = "The back end's transcript could not all be read, and what follows is left out: {sentence}"


def dumps(value):
    """The JSON bytes of `value` (UTF-8; a lone surrogate written as its escape); UNWRITABLE's for a value JSON cannot hold (a number that is not finite, a nesting too deep)."""
    try:
        try:
            return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except UnicodeEncodeError:
            return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
    except (ValueError, TypeError, RecursionError):
        return json.dumps(UNWRITABLE).encode("ascii")


def line_value(line):
    """One line of the transcript as the document holds it: the JSON object it is (read by `framing.decode`: finite numbers, no repeated key, at most 32 levels deep), else the line as text (invalid UTF-8 replaced)."""
    try:
        return framing.decode(line)
    except Exception:
        return line.decode("utf-8", errors="replace")


def meta_of(chat, hello):
    """The document's `chat` from the chat's entry of `chat.list` (checked by `assistant`) and the answer of `hello` (None when it failed: the back end is then null)."""
    return {"id": chat["id"], "title": chat["title"], "model": chat["model"], "backend": hello["backend"] if hello else None, "created": chat["created"]}


def iso_time(seconds):
    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class _Out:
    """The document's bytes on their way out: `put` adds an item within the budget (False when it would pass it), `force` adds a piece of the document's frame (from the reserve), `take` hands over what is gathered once there is enough (or all of it)."""

    def __init__(self, budget):
        self.budget, self.size, self.held, self.held_bytes = budget, 0, [], 0

    def put(self, data):
        if self.size + len(data) > self.budget:
            return False
        self.force(data)
        return True

    def force(self, data):
        self.held.append(data)
        self.size += len(data)
        self.held_bytes += len(data)

    def take(self, everything=False):
        if not self.held or (not everything and self.held_bytes < SEND_BYTES):
            return None
        data, self.held, self.held_bytes = b"".join(self.held), [], 0
        return data


def chunks(runner, chat_id, meta, *, ask, clock, deadline, seconds, events_max, bytes_max, now):
    """The document's bytes in pieces of about SEND_BYTES (a generator; each step blocks on the runner, so it runs in a worker). `runner` is the `assistant.Assistant`, `ask` the window's call wrapper (("ok", result) or ("error", the error)), `clock`/`deadline` the total time limit (`seconds` its length, for the sentence), `now` the wall clock of `exported_at`."""
    out = _Out(bytes_max - TAIL_RESERVE)
    head = dumps({"format": FORMAT, "exported_at": iso_time(now()), "chat": meta})
    out.force(head[:-1] + b',"events":[')
    stopped = None
    offset = count = 0
    while stopped is None:
        if clock() > deadline:
            stopped = STOPPED_TIME.format(minutes=seconds / 60)
            break
        asked = min(EVENTS_PAGE, events_max - count)
        if asked <= 0:
            kind, value = ask(lambda: runner.events(chat_id, offset, 1))
            if kind == "error":
                stopped = EVENTS_FAILED.format(sentence=words.chat_sentence(value))
            elif value[0]:
                stopped = STOPPED_EVENTS.format(count=events_max)
            break
        kind, value = ask(lambda: runner.events(chat_id, offset, asked))
        if kind == "error":
            stopped = EVENTS_FAILED.format(sentence=words.chat_sentence(value))
            break
        events, offset, _running = value
        if not events:
            break
        for event in events:
            if not out.put((b"\n" if count == 0 else b",\n") + dumps(event)):
                stopped = STOPPED_BYTES.format(size=f"{bytes_max / (1 << 20):g} MiB")
                break
            count += 1
        piece = out.take()
        if piece:
            yield piece
    out.force(b"\n]" if count else b"]")
    out.force(b',\n"backend_transcript":')
    if stopped is not None:
        out.force(b"null")
    else:
        stopped = yield from _transcript(out, runner, chat_id, ask=ask, clock=clock, deadline=deadline, seconds=seconds, bytes_max=bytes_max)
    out.force(b',\n"stopped":' + dumps(stopped) + b"}\n")
    yield out.take(everything=True)


def _transcript(out, runner, chat_id, *, ask, clock, deadline, seconds, bytes_max):
    """Write the `backend_transcript` value (a list of lines, or null) into `out`, yielding the pieces gathered on the way; returns the sentence of why it stopped, or None."""
    kind, value = ask(lambda: runner.transcript(chat_id, 0))
    if kind == "error" or value is None:
        out.force(b"null")
        return TRANSCRIPT_FAILED.format(sentence=words.chat_sentence(value)) if kind == "error" else None
    out.force(b"[")
    stopped, pending, offset, lines = None, bytearray(), 0, 0

    def add(line):
        nonlocal lines
        if not out.put((b"\n" if lines == 0 else b",\n") + dumps(line_value(line))):
            return STOPPED_BYTES.format(size=f"{bytes_max / (1 << 20):g} MiB")
        lines += 1
        return None

    while True:
        data, _size, end = value
        pending += data
        offset += len(data)
        while stopped is None:
            cut = pending.find(b"\n")
            if cut < 0:
                break
            stopped = add(bytes(pending[:cut]))
            del pending[:cut + 1]
        if stopped is None and len(pending) > LINE_MAX_BYTES:
            stopped = STOPPED_LINE
        piece = out.take()
        if piece:
            yield piece
        if stopped is not None or end:
            break
        if not data:
            stopped = TRANSCRIPT_FAILED.format(sentence=words.chat_sentence(assistant.NotUnderstood()))
            break
        if clock() > deadline:
            stopped = STOPPED_TIME.format(minutes=seconds / 60)
            break
        kind, value = ask(lambda: runner.transcript(chat_id, offset))
        if kind == "error" or value is None:
            stopped = TRANSCRIPT_FAILED.format(sentence=words.chat_sentence(value if kind == "error" else client.BrokerError("not_found", "gone")))
            break
    if stopped is None and pending:
        stopped = add(bytes(pending))
    out.force(b"\n]" if lines else b"]")
    return stopped


class ExportStream(Response):
    """The answer that sends the document of `chunks` (a generator, each step run in a worker) as the attachment `chat-<id>.json` of type `application/json`, under `Content-Security-Policy: sandbox`, without a length. A client that goes away ends it; `release()` is called when it ends, however it ends."""

    media_type = "application/json"

    def __init__(self, chat_id, pieces, release):
        # Not `Response.__init__`: with no body it would announce a length of 0 (the length is not known before the end).
        self.status_code, self.background = 200, None
        self.init_headers({"content-disposition": f'attachment; filename="chat-{chat_id}.json"', "content-security-policy": "sandbox"})
        self.pieces, self.release = pieces, release

    async def __call__(self, scope, receive, send):
        try:
            async with anyio.create_task_group() as group:

                async def watch():
                    while True:
                        if (await receive())["type"] == "http.disconnect":
                            group.cancel_scope.cancel()
                            return

                group.start_soon(watch)
                await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
                while True:
                    piece = await anyio.to_thread.run_sync(next, self.pieces, None)
                    if piece is None:
                        break
                    await send({"type": "http.response.body", "body": piece, "more_body": True})
                await send({"type": "http.response.body", "body": b"", "more_body": False})
                group.cancel_scope.cancel()
        finally:
            try:
                self.pieces.close()
            except ValueError:
                # Still running in its worker: it ends with that step.
                pass
            self.release()
