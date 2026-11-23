#!/usr/bin/env python3
"""A check of the web UI's settings box, themes and Chat window keys and columns in a real browser, run by hand against a running page: `PYTHON tests/e2e/manual/check_chat_ui_browser.py https://127.0.0.1:<port>/ [<folder for screenshots>]` with the management password on standard input, where PYTHON has Playwright and its Chromium, and `PLAYWRIGHT_BROWSERS_PATH` is set to the folder of its browsers. The page must have at least one source and a database with an action form; the chat's backend must answer a question (any answer that ends its turn).

It is not collected by pytest (no test can run the page's script). In headless Chromium at 1440 x 900 (the self-signed certificate accepted for this one context), by clicking and typing:

- the settings box: the gear opens it (`aria-expanded`, the focus inside), Escape closes it and gives the focus back to the gear, a click outside closes it; the theme is dark by default, light once chosen and after a reload, and follows the browser's scheme when "Follow the browser" is chosen; the default model is kept in `sdp.defaultModel`;
- every page (the Overview, a source page, Queue, Chat, a chat, a confirm page and the login) in both themes: no Content-Security-Policy violation, no console error, no failed request of the page's own origin; a screenshot of each when a folder is given;
- the conversation's scroller: only `.convo` scrolls, it opens at its bottom with the answer box (which follows the conversation) in view, and a view scrolled up stays where it is;
- the answer box: a box of blanks and Enter sends nothing, Shift+Enter makes a new line, an Enter while an input method composes (`isComposing`, and `keyCode` 229) does nothing, Enter sends the question, and the turn totals of the right column are asked again once the turn has ended;
- the right column: the model picker is in it; Hide gives the conversation its width (two grid columns), the handle covers no part of the conversation or the answer box, the state survives a reload, the handle shows it again; the sandbox's facts stay open after a reload once opened.

Standard library and Playwright only.
"""

import os
import sys
import urllib.parse

from playwright.sync_api import sync_playwright

WATCH = """
window.__sdpViolations = [];
document.addEventListener('securitypolicyviolation', function (event) {
  window.__sdpViolations.push(event.violatedDirective + ' ' + event.blockedURI);
});
"""
COMPOSING = """(mode) => {
  const box = document.querySelector('.cw form.send textarea');
  const event = new KeyboardEvent('keydown', {key: 'Enter', bubbles: true, cancelable: true, isComposing: mode === 'composing'});
  if (mode === '229') { Object.defineProperty(event, 'keyCode', {get: () => 229}); }
  box.dispatchEvent(event);
  return event.defaultPrevented;
}"""
COLUMNS = "() => getComputedStyle(document.querySelector('.cw')).gridTemplateColumns.split(' ').length"
AT_BOTTOM = "() => { const box = document.querySelector('.cw .convo'); return box.scrollHeight - box.scrollTop - box.clientHeight <= 40; }"
BOX_SEEN = "() => { const box = document.querySelector('.cw .convo').getBoundingClientRect(); const answer = document.querySelector('.cw .convo .composer').getBoundingClientRect(); return answer.top >= box.top && answer.bottom <= box.bottom + 1; }"
MID_STILL = "() => { const mid = document.querySelector('.cw .mid'); return mid.scrollHeight <= mid.clientHeight + 1; }"
UNDER_HANDLE = """() => {
  const handle = document.querySelector('.panel-handle').getBoundingClientRect();
  const parts = document.querySelectorAll('.cw .chat > *, .cw .convo .composer > *, .cw .composer-line');
  return Array.prototype.filter.call(parts, function (part) {
    const box = part.getBoundingClientRect();
    return box.width > 0 && box.left < handle.right && box.right > handle.left && box.top < handle.bottom && box.bottom > handle.top;
  }).length;
}"""


def main():
    if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        print("PLAYWRIGHT_BROWSERS_PATH must be set to the folder of Playwright's browsers", file=sys.stderr)
        return 2
    base = sys.argv[1].rstrip("/") + "/"
    shots = sys.argv[2] if len(sys.argv) > 2 else None
    if shots:
        os.makedirs(shots, exist_ok=True)
    password = sys.stdin.readline().rstrip("\n")
    origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlsplit(base))
    problems = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(ignore_https_errors=True, viewport={"width": 1440, "height": 900}, color_scheme="dark")
        context.add_init_script(WATCH)
        page = context.new_page()
        page.on("console", lambda message: problems.append(f"console {message.type}: {message.text}") if message.type == "error" else None)
        page.on("requestfailed", lambda request: problems.append(f"request failed: {request.url} {request.failure}") if request.url.startswith(origin) and request.failure != "net::ERR_ABORTED" else None)

        def violations():
            problems.extend(f"CSP violation on {page.url}: {entry}" for entry in page.evaluate("window.__sdpViolations || []"))

        def expect(condition, what):
            print(f"  {'ok' if condition else 'PROBLEM'}: {what}")
            if not condition:
                problems.append(what)

        def wait_until(script, arg=None, seconds=30):
            """Poll `script` (a function, evaluated with `arg`) until it is true: wait_for_function evaluates a string, which the page's policy refuses."""
            for _ in range(int(seconds * 5)):
                if page.evaluate(script, arg):
                    return True
                page.wait_for_timeout(200)
            return False

        def theme():
            return page.evaluate("document.documentElement.dataset.theme")

        def visit(path, name, scheme):
            page.goto(base + path.lstrip("/"))
            page.wait_for_load_state("networkidle")
            violations()
            if shots:
                page.screenshot(path=os.path.join(shots, f"{name}-{scheme}.png"))

        page.goto(base + "login")
        expect(theme() == "dark", "the login page is dark by default")
        page.fill("input[name=password]", password)
        page.click("button[type=submit]")
        page.wait_for_load_state("networkidle")

        # ── the settings box ──
        gear, box = page.locator("#settings-button"), page.locator("#settings-box")
        expect(theme() == "dark" and box.is_hidden() and gear.get_attribute("aria-expanded") == "false", "dark by default, the settings box closed")
        gear.click()
        expect(box.is_visible() and gear.get_attribute("aria-expanded") == "true" and page.evaluate("document.getElementById('settings-box').contains(document.activeElement)"), "the gear opens the box and the focus goes into it")
        page.keyboard.press("Escape")
        expect(box.is_hidden() and gear.get_attribute("aria-expanded") == "false" and page.evaluate("document.activeElement.id") == "settings-button", "Escape closes it and the focus is back on the gear")
        gear.click()
        page.mouse.click(700, 600)
        expect(box.is_hidden(), "a click outside closes it")
        gear.click()
        page.select_option("#setting-theme", "light")
        page.select_option("#setting-model", "sonnet")
        expect(theme() == "light", "choosing Light draws the light theme at once")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(theme() == "light" and page.input_value("#setting-theme") == "light", "the theme is kept after a reload")
        expect(page.evaluate("localStorage.getItem('sdp.defaultModel')") == "sonnet" and page.input_value("#setting-model") == "sonnet", "the default model is kept as sdp.defaultModel")
        gear.click()
        page.select_option("#setting-theme", "system")
        page.emulate_media(color_scheme="dark")
        expect(theme() == "dark", "Follow the browser: dark while the browser is dark")
        page.emulate_media(color_scheme="light")
        expect(wait_until("() => document.documentElement.dataset.theme === 'light'", None, 5), "Follow the browser: light once the browser turns light, without a reload")
        page.keyboard.press("Escape")

        # ── every page in both themes ──
        page.goto(base + "sources")
        source = page.get_attribute("a[href^='/sources/']", "href")
        page.goto(base + "databases")
        page.click("form[action='/pending'] button[type=submit] >> nth=0")
        page.wait_for_load_state("networkidle")
        pending = urllib.parse.urlsplit(page.url).path
        expect(pending.startswith("/pending/"), f"a database's action leads to its confirm page ({pending})")
        page.goto(base + "chat")
        page.wait_for_load_state("networkidle")
        with page.expect_request(lambda request: request.method == "POST" and request.url.endswith("/chat/new")) as made:
            page.click("aside.left form[action='/chat/new'] button")
        page.wait_for_load_state("networkidle")
        chat = urllib.parse.urlsplit(page.url).path
        expect(chat.startswith("/chat/c_"), f"New chat leads to {chat}")
        if page.locator("input.default-model").count():
            expect(urllib.parse.parse_qs(made.value.post_data or "").get("model") == ["sonnet"], "New chat sends this browser's default model (sonnet) once")
        page.evaluate("localStorage.setItem('sdp.defaultModel', '')")
        for scheme in ("dark", "light"):
            page.evaluate(f"localStorage.setItem('sdp.theme', '{scheme}')")
            for path, name in (("/", "overview"), (source, "source"), ("/queue", "queue"), ("/chat", "chat"), (chat, "chat-conversation"), (pending, "confirm")):
                visit(path, name, scheme)
                expect(theme() == scheme, f"{name} is {scheme}")

        # ── the answer box ──
        page.goto(base + chat.lstrip("/"))
        page.wait_for_load_state("networkidle")
        page.evaluate("window.__sdpMark = 'not reloaded'")
        before = page.locator(".cw .chat .msg.user").count()
        expect(page.evaluate(MID_STILL), "only the conversation's scroller scrolls, not its column")
        expect(page.evaluate(AT_BOTTOM) and page.evaluate(BOX_SEEN), "the conversation opens at its bottom with the answer box in view")
        box_text = page.locator(".cw form.send textarea")
        expect("Enter to send, Shift+Enter for a new line" in (box_text.get_attribute("placeholder") or ""), "the box says what Enter and Shift+Enter do")
        box_text.fill("   \n  ")
        box_text.press("Enter")
        page.wait_for_timeout(600)
        expect(page.evaluate("window.__sdpMark") == "not reloaded" and box_text.input_value() == "   \n  ", "a box of blanks: Enter sends nothing")
        box_text.fill("first line")
        box_text.press("Shift+Enter")
        box_text.type("second line")
        expect(box_text.input_value() == "first line\nsecond line", "Shift+Enter makes a new line")
        expect(page.evaluate(COMPOSING, "composing") is False and page.evaluate(COMPOSING, "229") is False, "an Enter of an input method (isComposing, keyCode 229) is left alone")
        page.wait_for_timeout(600)
        expect(page.evaluate("window.__sdpMark") == "not reloaded", "nothing was sent while composing")
        totals_before = page.inner_text("#chat-totals")
        picker = page.locator("#chat-model")
        expect(picker.count() == page.locator("aside.rightp #chat-model").count(), "the model picker is in This chat")
        if picker.count():
            picker.select_option("haiku")
        with page.expect_navigation(), page.expect_request(lambda request: request.method == "POST" and request.url.endswith("/send")) as sent:
            box_text.press("Enter")
        fields = urllib.parse.parse_qs(sent.value.post_data or "", keep_blank_values=True)
        expect(fields.get("text") == ["first line\nsecond line"] or fields.get("text") == ["first line\r\nsecond line"], "the question was sent whole")
        if picker.count():
            expect(fields.get("model") == ["haiku"], f"the picker of This chat posts the model once ({fields.get('model')})")
        page.wait_for_load_state("networkidle")
        expect(page.evaluate("window.__sdpMark") is None and page.locator(".cw .chat .msg.user").count() == before + 1, "Enter sent the question (the page was posted and shows it)")
        ends = page.locator(".cw .chat .chat-end").count()
        page.evaluate("document.querySelector('.cw .convo').scrollTop = 0")
        expect(wait_until("(ends) => document.querySelectorAll('.cw .chat .chat-end').length > ends", ends, 180), "the turn ended without a reload")
        expect(page.evaluate("document.querySelector('.cw .convo').scrollTop") == 0, "new events did not move a view scrolled up")
        changed = wait_until("(old) => { const part = document.getElementById('chat-totals'); return part !== null && part.innerText !== old && !part.innerText.includes('Adding up'); }", totals_before)
        expect(changed, f"the turn totals were asked again once the turn ended ({totals_before!r} -> {page.inner_text('#chat-totals')!r})")
        violations()

        # ── data in reach and the chart ──
        expect(wait_until("() => { const part = document.getElementById('chat-reach'); return part !== null && !part.hasAttribute('hx-get') && !part.innerText.includes('checking'); }", None, 30), f"data in reach is filled in ({page.inner_text('#chat-reach') if page.locator('#chat-reach').count() else 'no block'!r})")
        if page.locator(".vl-chart").count():
            drawn = wait_until("() => document.querySelector('.vl-chart svg') !== null", None, 30)
            expect(drawn, "the chart is drawn")
            colour = "() => { const mark = document.querySelector('.vl-chart svg text'); return mark ? mark.getAttribute('fill') || getComputedStyle(mark).fill : null; }"
            before_colour = page.evaluate(colour)
            if shots:
                page.locator(".vl-chart").first.scroll_into_view_if_needed()
                page.screenshot(path=os.path.join(shots, f"chart-{theme()}.png"))
            other = "light" if theme() == "dark" else "dark"
            page.click("#settings-button")
            page.select_option("#setting-theme", other)
            page.keyboard.press("Escape")
            changed = wait_until("(old) => { const mark = document.querySelector('.vl-chart svg text'); return mark !== null && (mark.getAttribute('fill') || getComputedStyle(mark).fill) !== old; }", before_colour, 10)
            expect(changed, f"the chart is drawn again in the {other} theme ({before_colour} -> {page.evaluate(colour)})")
            if shots:
                page.locator(".vl-chart").first.scroll_into_view_if_needed()
                page.screenshot(path=os.path.join(shots, f"chart-{other}.png"))
        violations()

        # ── the right column ──
        expect(page.evaluate(COLUMNS) == 3 and page.locator("aside.rightp").is_visible(), "three columns with This chat open")
        page.click("[data-chat-panel='close']")
        expect(page.evaluate(COLUMNS) == 2 and page.locator("aside.rightp").is_hidden() and page.locator(".panel-handle").is_visible(), "Hide gives the conversation the width; the handle is shown")
        for where in ("0", "document.querySelector('.cw .convo').scrollHeight"):
            page.evaluate(f"document.querySelector('.cw .convo').scrollTop = {where}")
            expect(page.evaluate(UNDER_HANDLE) == 0, f"the handle covers no part of the conversation (scrolled to {where})")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(page.evaluate(COLUMNS) == 2 and page.get_attribute(".panel-handle", "aria-expanded") == "false", "the column stays hidden after a reload")
        page.click(".panel-handle")
        expect(page.evaluate(COLUMNS) == 3 and page.locator("aside.rightp").is_visible(), "the handle shows the column again")
        page.click("details.sandbox > summary")
        # The details' toggle event, which keeps the state, comes a moment after the click.
        wait_until("() => localStorage.getItem('sdp.sandboxOpen') === '1'", None, 5)
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(page.evaluate("document.querySelector('details.sandbox').open") is True, "the sandbox's facts stay open after a reload")
        page.click("details.sandbox > summary")
        violations()

        # ── the login page in both themes ──
        page.click("form.logout button")
        page.wait_for_load_state("networkidle")
        for scheme in ("dark", "light"):
            page.evaluate(f"localStorage.setItem('sdp.theme', '{scheme}')")
            visit("/login", "login", scheme)
            expect(theme() == scheme, f"login is {scheme}")
        browser.close()
    if problems:
        for problem in problems:
            print(f"  PROBLEM: {problem}")
        return 1
    print("  no CSP violation, no console error, no failed request")
    return 0


if __name__ == "__main__":
    sys.exit(main())
