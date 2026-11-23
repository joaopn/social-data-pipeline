#!/usr/bin/env python3
"""A check of the Chat window's interactive charts (`static/plots.js`, `static/chat.css`) in a real browser, run by hand: `PYTHON tests/e2e/manual/check_charts_browser.py <folder with vega.min.js, vega-lite.min.js and vega-embed.min.js> [<folder for screenshots>]`, where PYTHON has Playwright and its Chromium, and `PLAYWRIGHT_BROWSERS_PATH` is set to the folder of its browsers. The three files must be the pinned ones (their SHA-256 sums are read from Dockerfile.webui).

It is not collected by pytest (no test can run the page's script). It needs no stack: it serves a page of plot cards (the markup of `_chat_macros.html`) in a column narrower than the wide charts on 127.0.0.1, with the web UI's own static files and content policy (read from `webui/guard.py`), and every spec as `vlspec.check` passes it. In headless Chromium at 1440 x 900:

- a two-panel chart 900 px wide is scaled into the column (its SVG no wider than the column, no horizontal scroll);
- on a scaled scatter chart with scale-bound zoom and pan, hovering each point shows that point's tooltip, a drag of 100 px moves the points 100 px, and a wheel zoom keeps the point under the mouse where it was;
- a chart with a log y scale (a single view, a layer whose layers carry it, a vertical concatenation, a facet with `symlog`) gets the "Linear / Log" switch, "Log" pressed: "Linear" draws its y axes linear (the axis says so, its ticks are evenly spaced), "Log" draws them as the spec has them again, the card keeping its height (also on every frame while it is drawn again); a chart without a log y scale gets no switch;
- no Content-Security-Policy violation, no console error.

Standard library and Playwright only (`vlspec` is standard library too).
"""

import hashlib
import http.server
import json
import os
import re
import shutil
import sys
import tempfile
import threading

from playwright.sync_api import sync_playwright

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO)
from social_data_pipeline.webui import vlspec  # noqa: E402

STATIC = os.path.join(REPO, "social_data_pipeline", "webui", "static")
VENDOR = ("vega.min.js", "vega-lite.min.js", "vega-embed.min.js")
SCHEMA = "https://vega.github.io/schema/vega-lite/v6.json"
WATCH = """
window.__sdpViolations = [];
document.addEventListener('securitypolicyviolation', function (event) {
  window.__sdpViolations.push(event.violatedDirective + ' ' + event.blockedURI);
});
"""
PAGE = """<!doctype html>
<html lang="en" data-theme="dark"><head><meta charset="utf-8"><title>charts</title>
<link rel="stylesheet" href="/static/app.css"><link rel="stylesheet" href="/static/chat.css"><link rel="stylesheet" href="/check.css">
<script src="/static/vendor/vega.min.js" defer></script><script src="/static/vendor/vega-lite.min.js" defer></script><script src="/static/vendor/vega-embed.min.js" defer></script><script src="/static/plots.js" defer></script>
</head><body><div class="cw"><div class="chat">{cards}</div></div></body></html>
"""
CARD = '<div class="plot chat-file" id="{name}"><h3>{name}.vl.json</h3><div class="sub">1 KiB</div><div class="vl-chart" data-vl-spec="/spec/{name}.vl.json"></div><p class="vl-note small muted" hidden></p></div>'
CHECK_CSS = ".cw .chat { width: 700px; margin: 20px; display: block; overflow: visible; }\n"
POINTS = "(name) => Array.from(document.querySelectorAll('#' + name + ' .mark-symbol path')).map(e => { const r = e.getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2, e.__data__.datum.x]; })"
Y_AXES = "(name) => Array.from(document.querySelectorAll('#' + name + ' g[aria-roledescription=\"axis\"]')).map(g => g.getAttribute('aria-label') || '').filter(l => l.startsWith('Y-axis'))"
Y_TICKS = "(name) => { const g = Array.from(document.querySelectorAll('#' + name + ' g[aria-roledescription=\"axis\"]')).filter(g => (g.getAttribute('aria-label') || '').startsWith('Y-axis'))[0]; return Array.from(g.querySelectorAll('.role-axis-label text')).map(t => [t.textContent, t.getBoundingClientRect().y + t.getBoundingClientRect().height / 2]); }"
WATCH_HEIGHT = "(name) => { const heights = window.__heights = []; const card = document.querySelector('#' + name); let frames = 60; (function look() { heights.push(card.getBoundingClientRect().height); if (--frames > 0) requestAnimationFrame(look); })(); }"
PRESSED = "(name) => Array.from(document.querySelectorAll('#' + name + ' .vl-scale button')).map(b => b.textContent + '=' + b.getAttribute('aria-pressed'))"


def specs():
    rising = [{"m": m, "n": 10 ** (m / 2), "k": "a" if m % 2 else "b"} for m in range(11)]
    line = {"mark": "line", "encoding": {"x": {"field": "m", "type": "quantitative"}, "y": {"field": "n", "type": "quantitative", "scale": {"type": "log"}}}}
    return {
        "wide": {"$schema": SCHEMA, "data": {"values": rising}, "vconcat": [dict(line, width=900, height=120, encoding=dict(line["encoding"], y={"field": "n", "type": "quantitative"})), {"width": 900, "height": 120, "mark": "bar", "encoding": {"x": {"field": "m", "type": "ordinal"}, "y": {"field": "n", "type": "quantitative"}}}]},
        "scatter": {"$schema": SCHEMA, "width": 900, "height": 300, "data": {"values": [{"x": i, "y": (i * 7) % 13 + 1} for i in range(21)]}, "params": [{"name": "grid", "select": "interval", "bind": "scales"}], "mark": {"type": "point", "filled": True, "size": 120}, "encoding": {"x": {"field": "x", "type": "quantitative"}, "y": {"field": "y", "type": "quantitative"}, "tooltip": [{"field": "x"}, {"field": "y"}]}},
        "single": dict({"$schema": SCHEMA, "width": 400, "height": 200, "data": {"values": rising}}, **line),
        "layered": {"$schema": SCHEMA, "width": 400, "height": 200, "data": {"values": rising}, "layer": [line, {"mark": "point", "encoding": line["encoding"]}]},
        "panels": {"$schema": SCHEMA, "data": {"values": rising}, "vconcat": [dict(line, width=400, height=120), {"width": 400, "height": 120, "mark": "point", "encoding": {"x": {"field": "m", "type": "ordinal"}, "y": {"field": "n", "type": "quantitative", "scale": {"type": "log"}}}}]},
        "faceted": {"$schema": SCHEMA, "data": {"values": rising}, "facet": {"column": {"field": "k"}}, "spec": {"width": 250, "height": 150, "mark": "line", "encoding": {"x": {"field": "m", "type": "quantitative"}, "y": {"field": "n", "type": "quantitative", "scale": {"type": "symlog"}}}}},
    }


def pinned_sums():
    text = open(os.path.join(REPO, "Dockerfile.webui"), encoding="utf-8").read()
    return {name: digest for digest, name in re.findall(r"([0-9a-f]{64})\s+(vega[\w.-]*\.min\.js)", text)}


def serve(root, policy):
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=root, **kwargs)

        def end_headers(self):
            self.send_header("Content-Security-Policy", policy)
            super().end_headers()

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def build(root, vendor):
    shutil.copytree(STATIC, os.path.join(root, "static"))
    os.makedirs(os.path.join(root, "static", "vendor"), exist_ok=True)
    os.makedirs(os.path.join(root, "spec"))
    sums = pinned_sums()
    for name in VENDOR:
        data = open(os.path.join(vendor, name), "rb").read()
        if hashlib.sha256(data).hexdigest() != sums.get(name):
            raise SystemExit(f"{name} is not the pinned file (its SHA-256 differs from Dockerfile.webui)")
        open(os.path.join(root, "static", "vendor", name), "wb").write(data)
    for name, spec in specs().items():
        text, reason = vlspec.check(json.dumps(spec).encode())
        if reason:
            raise SystemExit(f"vlspec refused the spec {name}: {reason}")
        open(os.path.join(root, "spec", f"{name}.vl.json"), "w", encoding="ascii").write(text)
    open(os.path.join(root, "check.css"), "w").write(CHECK_CSS)
    open(os.path.join(root, "index.html"), "w").write(PAGE.format(cards="".join(CARD.format(name=name) for name in specs())))


def evenly_spaced(ticks):
    values = [(float(text.replace(",", "")), y) for text, y in ticks if text]
    if len(values) < 3:
        return False
    (v0, y0), (v1, y1) = values[0], values[-1]
    return all(abs(y0 + (v - v0) * (y1 - y0) / (v1 - v0) - y) < 1.5 for v, y in values)


def main():
    if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        print("PLAYWRIGHT_BROWSERS_PATH must be set to the folder of Playwright's browsers", file=sys.stderr)
        return 2
    vendor = sys.argv[1]
    shots = sys.argv[2] if len(sys.argv) > 2 else None
    if shots:
        os.makedirs(shots, exist_ok=True)
    policy = re.search(r'^CONTENT_SECURITY_POLICY = "([^"]+)"', open(os.path.join(STATIC, "..", "guard.py"), encoding="utf-8").read(), re.M).group(1)
    root = tempfile.mkdtemp(prefix="sdp-charts-")
    problems = []
    try:
        build(root, vendor)
        server = serve(root, policy)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(viewport={"width": 1440, "height": 900})
            page.add_init_script(WATCH)
            page.on("console", lambda message: problems.append(f"console {message.type}: {message.text}") if message.type in ("error", "warning") else None)
            page.on("pageerror", lambda error: problems.append(f"page error: {error}"))

            def expect(condition, what):
                print(f"  {'ok' if condition else 'PROBLEM'}: {what}")
                if not condition:
                    problems.append(what)

            page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
            page.wait_for_function(f"document.querySelectorAll('.plot.vl-drawn').length === {len(specs())} || document.querySelector('.vl-note:not([hidden])')", timeout=20000)
            notes = page.evaluate("Array.from(document.querySelectorAll('.vl-note:not([hidden])')).map(n => n.textContent)")
            expect(not notes, f"every chart is drawn (notes: {notes})")

            print("fit:")
            for name in ("wide", "scatter"):
                widths = page.evaluate("(name) => [document.querySelector('#' + name + ' svg').getBoundingClientRect().width, document.querySelector('#' + name + ' .vl-chart').clientWidth, document.querySelector('#' + name + ' svg').getAttribute('viewBox')]", name)
                expect(widths[0] <= widths[1] + 0.5, f"{name}: its SVG ({widths[0]:.0f} px, viewBox {widths[2]}) fits the column ({widths[1]} px)")
            expect(page.evaluate("document.documentElement.scrollWidth <= document.documentElement.clientWidth"), "the page has no horizontal scroll")

            print("scaled scatter:")
            page.locator("#scatter").scroll_into_view_if_needed()
            points = page.evaluate(POINTS, "scatter")
            box = page.evaluate("(() => { const r = document.querySelector('#scatter svg').getBoundingClientRect(); return [r.left, r.top, r.right, r.bottom]; })()")
            misses = []
            for x, y, value in points:
                # A point at the domain's edge is half clipped (`clip` of the scale-bound selection): hover its drawn half.
                x, y = min(x, box[2] - 3), y
                page.mouse.move(5, 5)
                page.mouse.move(x, y + (2 if y < box[1] + 30 else 0), steps=3)
                page.wait_for_timeout(60)
                text = page.evaluate("(document.getElementById('vg-tooltip-element') || {}).textContent || ''")
                if not text.startswith(f"x{value}y"):
                    misses.append((value, text))
            expect(not misses, f"hovering each of {len(points)} points shows its own tooltip (misses: {misses})")
            unit = (max(p[0] for p in points) - min(p[0] for p in points)) / 20
            first = {p[2]: p[0] for p in points}
            middle = (first[8] + first[12]) / 2
            low = max(p[1] for p in points) + 4
            page.mouse.move(middle, low)
            page.mouse.down()
            page.mouse.move(middle + 50, low, steps=5)
            page.mouse.move(middle + 100, low, steps=5)
            page.mouse.up()
            page.wait_for_timeout(200)
            after = {p[2]: p[0] for p in page.evaluate(POINTS, "scatter")}
            expect(abs(after[10] - first[10] - 100) < 2, f"a 100 px drag moves the points {after[10] - first[10]:.1f} px (a scale unit is {unit:.1f} px)")
            anchor = after[6]
            page.mouse.move(anchor, low - 10)
            page.mouse.wheel(0, -300)
            page.wait_for_timeout(300)
            zoomed = {p[2]: p[0] for p in page.evaluate(POINTS, "scatter")}
            expect(abs(zoomed[6] - anchor) < 2 and abs(zoomed[10] - zoomed[6]) > abs(after[10] - after[6]) + 5, f"a wheel zoom at a point keeps it under the mouse (moved {zoomed[6] - anchor:.1f} px) and spreads the points")

            print("log/linear switch:")
            for name in ("wide", "scatter"):
                expect(page.locator(f"#{name} .vl-scale").count() == 0, f"{name}: no log y scale, no switch")
            for name in ("single", "layered", "panels", "faceted"):
                logged = "symlog" if name == "faceted" else "log"
                expect(page.evaluate(PRESSED, name) == ["Linear=false", "Log=true"], f"{name}: the switch is shown, Log pressed ({page.evaluate(PRESSED, name)})")
                if not page.evaluate(PRESSED, name):
                    continue
                before = page.evaluate(Y_AXES, name)
                ticks = page.evaluate(Y_TICKS, name)
                height = page.evaluate("(name) => document.querySelector('#' + name).getBoundingClientRect().height", name)
                expect(any(f"for a {logged} scale" in label for label in before), f"{name}: drawn on a {logged} y scale, ticks {[t for t, _ in ticks if t]}")
                page.evaluate(WATCH_HEIGHT, name)
                page.locator(f"#{name} .vl-scale button", has_text="Linear").click()
                page.wait_for_function("(name) => Array.from(document.querySelectorAll('#' + name + ' g[aria-roledescription=\"axis\"]')).some(g => (g.getAttribute('aria-label') || '').indexOf('for a linear scale') > 0 && g.getAttribute('aria-label').startsWith('Y-axis'))", arg=name, timeout=10000)
                linear = page.evaluate(Y_AXES, name)
                ticks = page.evaluate(Y_TICKS, name)
                expect(all("for a linear scale" in label for label in linear) and page.evaluate(PRESSED, name) == ["Linear=true", "Log=false"], f"{name}: Linear draws every y axis linear ({len(linear)} axes), ticks {[t for t, _ in ticks if t]}")
                lowest = page.evaluate("Math.min.apply(null, window.__heights.splice(0))")
                expect(lowest >= height - 1, f"{name}: the card does not shrink while it is drawn again (lowest {lowest:.0f} px of {height:.0f} px)")
                expect(evenly_spaced(ticks), f"{name}: the linear ticks are evenly spaced")
                expect(page.locator(f"#{name} .vl-chart > .vega-embed").count() == 1, f"{name}: one view in the card after the switch")
                if shots and name == "single":
                    page.locator("#single").screenshot(path=os.path.join(shots, "chart-linear.png"))
                page.locator(f"#{name} .vl-scale button", has_text="Log").click()
                page.wait_for_function("(name) => Array.from(document.querySelectorAll('#' + name + ' g[aria-roledescription=\"axis\"]')).filter(g => (g.getAttribute('aria-label') || '').startsWith('Y-axis')).every(g => g.getAttribute('aria-label').indexOf('for a linear scale') < 0)", arg=name, timeout=10000)
                expect(page.evaluate(Y_AXES, name) == before, f"{name}: Log draws the y axes as the spec has them again")
                again = page.evaluate("(name) => document.querySelector('#' + name).getBoundingClientRect().height", name)
                expect(abs(again - height) < 1, f"{name}: the card keeps its height ({height:.0f} px, then {again:.0f} px)")
            if page.evaluate(PRESSED, "single"):
                page.locator("#single .vl-scale button", has_text="Linear").click()
            page.wait_for_timeout(300)
            page.evaluate("document.documentElement.setAttribute('data-theme', 'light')")
            page.wait_for_timeout(800)
            expect(page.evaluate(PRESSED, "single") == ["Linear=true", "Log=false"] and all("for a linear scale" in label for label in page.evaluate(Y_AXES, "single")), "after a theme change the card keeps its choice (Linear)")
            expect(page.locator("#single .vl-chart > .vega-embed").count() == 1, "one view in the card after the theme change")
            if shots:
                page.screenshot(path=os.path.join(shots, "charts-light.png"), full_page=True)
                page.evaluate("document.documentElement.setAttribute('data-theme', 'dark')")
                page.wait_for_timeout(800)
                page.screenshot(path=os.path.join(shots, "charts-dark.png"), full_page=True)
            violations = page.evaluate("window.__sdpViolations")
            expect(not violations, f"no Content-Security-Policy violation ({violations})")
            browser.close()
        server.shutdown()
    finally:
        shutil.rmtree(root, ignore_errors=True)
    if problems:
        print("PROBLEMS:\n  " + "\n  ".join(problems))
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
