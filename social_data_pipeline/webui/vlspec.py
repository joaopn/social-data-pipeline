"""The check of a chart spec the assistant wrote (`<name>.vl.json`) before the Chat window sends it to the page, where `static/plots.js` draws it with Vega-Lite.

The spec is the runner's output, so hostile text: anything the browser would do because of it must be something the check allowed. The page's content policy already stops a request to another site, inline code and `eval` (the page draws with Vega's expression interpreter); what the policy does not stop, and the check refuses, is a request to this web UI's own routes with the operator's cookie (data loaded from an address), a navigation away carrying data (a mark's link), a control placed elsewhere in the page (a binding's `element`), an option of the embedding library set from the spec (`usermeta`, dropped), and a spec too large or too deep to draw. What `check` accepts:

- at most `BYTES_MAX` bytes of strict JSON: no NaN or Infinity, no number out of a float's range nor a whole number beyond 2**53, no repeated key in an object (a check that sees one value where the browser takes another), nested at most `DEPTH_MAX` deep and at most `CONTAINERS_MAX` objects and lists in all (counted in one pass over the bytes before it is parsed);
- a top-level object that is a Vega-Lite view (a `mark`, a `layer`, a concatenation, a `facet` or a `repeat`), its `$schema`, when present, Vega-Lite's own schema address and only at the top;
- no key `url`, `href`, `loader` or `element` anywhere in the spec, no `image` mark, no key `__proto__`, `constructor` or `prototype` anywhere, the data included;
- data only inline: a `data` is `null` or an object of `values` (a list of rows or one object), `name` (a dataset of the top-level `datasets`), `format`, `sequence` (at most `ROWS_MAX` numbers), `graticule` or `sphere`; at most `ROWS_MAX` rows in all and at most `VIEWS_MAX` views (layers, concatenated views, repeated views);
- `usermeta` is dropped wherever it is (vega-embed reads its options from it).

The answer is the spec serialized again by Python (nothing of the file's bytes is passed on as they were), or the reason it was refused, a sentence part the page completes ("This chart could not be drawn: <reason>; ..."). Standard library only.
"""

import json
import math
import re

BYTES_MAX = 2 * 1024 * 1024
DEPTH_MAX = 64
CONTAINERS_MAX = 200_000
ROWS_MAX = 50_000
VIEWS_MAX = 100
# The largest whole number the browser holds exactly; a larger one is refused as out of range.
INT_MAX = 2 ** 53
SCHEMA = re.compile(r"https://vega\.github\.io/schema/vega-lite/v[0-9]{1,3}(?:\.[0-9]{1,4}){0,2}(?:-[A-Za-z0-9.]{1,20})?\.json")
# Keys refused anywhere in the spec, with the reason; the data's rows are not looked at for these (a row may well have a column named url).
REFUSED_KEYS = {
    "url": "it loads data from an address",
    "href": "it links its marks to addresses",
    "loader": "it sets a data loader",
    "element": "it places a control outside the chart",
}
# Keys refused anywhere, the data's rows included: names that reach an object's prototype in the browser.
PROTOTYPE_KEYS = frozenset({"__proto__", "constructor", "prototype"})
VIEW_KEYS = ("mark", "layer", "concat", "hconcat", "vconcat", "facet", "repeat")
DATA_KEYS = frozenset({"values", "name", "format", "sequence", "graticule", "sphere"})
LIST_VIEWS = ("layer", "concat", "hconcat", "vconcat")

TOO_LARGE = "its file is larger than 2 MiB"
NOT_JSON = "it is not strict JSON (a NaN, an Infinity, a number out of range or a repeated key)"
TOO_DEEP = f"it is nested more than {DEPTH_MAX} levels deep"
TOO_MANY = f"it holds more than {CONTAINERS_MAX:,} objects and lists"
NOT_VEGA_LITE = "it is not a Vega-Lite chart"
IMAGE = "it draws images"
NOT_INLINE = "its data is not rows held in the spec"
NO_DATASET = "it names a dataset it does not hold"
TOO_MANY_ROWS = f"it holds more than {ROWS_MAX:,} rows of data"
TOO_MANY_VIEWS = f"it has more than {VIEWS_MAX} layers or views"
PROTOTYPE = "it has a key the page does not accept (__proto__, constructor or prototype)"

_STRUCTURE = re.compile(rb'["\\\[\]{}]')


class Refused(ValueError):
    """The spec is not drawn; `reason` says why."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def shape(payload):
    """(the deepest nesting, the number of objects and lists) of the JSON text `payload`, in one pass over the bytes that matter with an inside-string and an escaped-position state (as `assistant.containers` counts): linear whatever the text holds."""
    depth = deepest = count = 0
    inside, escaped_at = False, -1
    for found in _STRUCTURE.finditer(payload):
        position, character = found.start(), found.group()
        if inside:
            if position == escaped_at:
                continue
            if character == b"\\":
                escaped_at = position + 1
            elif character == b'"':
                inside = False
            continue
        if character == b'"':
            inside = True
        elif character in (b"[", b"{"):
            depth += 1
            count += 1
            deepest = max(deepest, depth)
        elif character in (b"]", b"}"):
            depth -= 1
    return deepest, count


def _pairs(pairs):
    found = {}
    for key, value in pairs:
        if key in found:
            raise ValueError("a repeated key")
        found[key] = value
    return found


def _constant(_name):
    raise ValueError("not a number of JSON")


def _float(text):
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("out of range")
    return value


def _int(text):
    value = int(text)
    if abs(value) > INT_MAX:
        raise ValueError("out of range")
    return value


def parse(payload):
    """The JSON text `payload` (bytes) parsed strictly, or Refused."""
    if len(payload) > BYTES_MAX:
        raise Refused(TOO_LARGE)
    deepest, count = shape(payload)
    if deepest > DEPTH_MAX:
        raise Refused(TOO_DEEP)
    if count > CONTAINERS_MAX:
        raise Refused(TOO_MANY)
    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant, parse_float=_float, parse_int=_int)
    except (ValueError, RecursionError):
        # A ValueError covers bad UTF-8, bad JSON, the hooks' refusals and an integer of more digits than Python converts.
        raise Refused(NOT_JSON) from None


def _rows(value):
    """The rows of an inline `values` or of a dataset: a list counts its items, one object (GeoJSON) one row; anything else is not inline rows."""
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        return 1
    raise Refused(NOT_INLINE)


def _sequence_rows(sequence):
    if not isinstance(sequence, dict) or not set(sequence) <= {"start", "stop", "step", "as"}:
        raise Refused(NOT_INLINE)
    start, stop, step = sequence.get("start"), sequence.get("stop"), sequence.get("step", 1)
    numbers = (start, stop, step)
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in numbers) or step == 0:
        raise Refused(NOT_INLINE)
    return max(0, math.ceil((stop - start) / step))


def _check_data(data, datasets):
    """The rows a `data` object adds, or Refused."""
    if data is None:
        return 0
    if isinstance(data, dict):
        for key in REFUSED_KEYS:
            if key in data:
                raise Refused(REFUSED_KEYS[key])
    if not isinstance(data, dict) or not set(data) <= DATA_KEYS:
        raise Refused(NOT_INLINE)
    if "values" in data:
        return _rows(data["values"])
    if "sequence" in data:
        return _sequence_rows(data["sequence"])
    if "name" in data:
        if not isinstance(data["name"], str) or data["name"] not in datasets:
            raise Refused(NO_DATASET)
        return 0
    if "graticule" in data or "sphere" in data:
        return 0
    raise Refused(NOT_INLINE)


def _is_image(mark):
    return mark == "image" or (isinstance(mark, dict) and mark.get("type") == "image")


def _repeated(repeat):
    if isinstance(repeat, list):
        return len(repeat)
    if isinstance(repeat, dict):
        product = 1
        for value in repeat.values():
            if isinstance(value, list):
                product *= max(1, len(value))
        return product
    return 1


def _no_prototype_keys(value):
    """Refused when an object anywhere under `value` (data rows included) has a prototype key; walked without recursion."""
    todo = [value]
    while todo:
        item = todo.pop()
        if isinstance(item, dict):
            if PROTOTYPE_KEYS.intersection(item):
                raise Refused(PROTOTYPE)
            todo.extend(item.values())
        elif isinstance(item, list):
            todo.extend(item)


def check_spec(spec):
    """The checked spec (a new top-level object without `usermeta` anywhere) of the parsed `spec`, or Refused."""
    if not isinstance(spec, dict) or not any(key in spec for key in VIEW_KEYS):
        raise Refused(NOT_VEGA_LITE)
    if "$schema" in spec and not (isinstance(spec["$schema"], str) and SCHEMA.fullmatch(spec["$schema"])):
        raise Refused(NOT_VEGA_LITE)
    _no_prototype_keys(spec)
    datasets = spec.get("datasets", {})
    if not isinstance(datasets, dict):
        raise Refused(NOT_INLINE)
    rows = sum(_rows(value) for value in datasets.values())
    views = 0
    top = {key: value for key, value in spec.items() if key not in ("usermeta", "datasets")}
    todo = [top]
    while todo:
        item = todo.pop()
        if isinstance(item, list):
            todo.extend(item)
            continue
        if not isinstance(item, dict):
            continue
        for key in [key for key in item if key == "usermeta"]:
            del item[key]
        for key, value in item.items():
            if key in REFUSED_KEYS:
                raise Refused(REFUSED_KEYS[key])
            if key == "$schema" and item is not top:
                raise Refused(NOT_VEGA_LITE)
            if key == "mark" and _is_image(value):
                raise Refused(IMAGE)
            if key == "data":
                rows += _check_data(value, datasets)
                if isinstance(value, dict) and isinstance(value.get("format"), dict):
                    todo.append(value["format"])
                continue
            if key in LIST_VIEWS and isinstance(value, list):
                views += len(value)
            elif key == "repeat":
                views += _repeated(value)
            todo.append(value)
        if rows > ROWS_MAX:
            raise Refused(TOO_MANY_ROWS)
        if views > VIEWS_MAX:
            raise Refused(TOO_MANY_VIEWS)
    if rows > ROWS_MAX:
        raise Refused(TOO_MANY_ROWS)
    if "datasets" in spec:
        top["datasets"] = datasets
    return top


def check(payload):
    """(the checked spec as JSON text, None) or (None, the reason it is refused) for the bytes of a `.vl.json` file."""
    try:
        spec = check_spec(parse(payload))
    except Refused as refused:
        return None, refused.reason
    return json.dumps(spec, ensure_ascii=True, allow_nan=False, separators=(",", ":")), None
