#!/usr/bin/env python3
"""A check of the web UI's settings box, themes and Chat window keys and columns in a real browser, run by hand against a running page: `PYTHON tests/e2e/manual/check_chat_ui_browser.py https://127.0.0.1:<port>/ [<folder for screenshots>]` with the management password on standard input, where PYTHON has Playwright and its Chromium, and `PLAYWRIGHT_BROWSERS_PATH` is set to the folder of its browsers. The page must have at least one source and a database with an action form; the chat's backend must answer a question (any answer that ends its turn), and generated chat titles must be on: a new chat's first question gets a generated title while its first turn runs.

It is not collected by pytest (no test can run the page's script). In headless Chromium at 1440 x 900 (the self-signed certificate accepted for this one context), by clicking and typing:

- the settings box: the gear opens it (`aria-expanded`, the focus inside), Escape closes it and gives the focus back to the gear, a click outside closes it; the theme is dark by default, light once chosen and after a reload, and follows the browser's scheme when "Follow the browser" is chosen; the default model is kept in `sdp.defaultModel`;
- every page (the Overview, a source page, Queue, Chat, a chat, a confirm page and the login) in both themes: no Content-Security-Policy violation, no console error, no failed request of the page's own origin; a screenshot of each when a folder is given;
- the conversation's scroller: only `.convo` scrolls, it opens at its bottom with the answer box (which follows the conversation) in view, and a view scrolled up stays where it is;
- the answer box: a box of blanks and Enter sends nothing, Shift+Enter makes a new line, an Enter while an input method composes (`isComposing`, and `keyCode` 229) does nothing, Enter sends the question, and the turn totals of the right column are asked again once the turn has ended;
- a generated title: after a new chat's first question, the chat list's label and the title's field change to the generated title without a reload, the conversation shows nothing of it (no "not understood" line) and the turn totals' Cost says it includes the title; while the field has the focus, or holds an edit, it is left as it is (the list still changes), and a reload shows the generated title in the field;
- the chat's title (the first line of the right column, a field of its own form): idle it has no box and no Save button, focused it shows Save; Enter saves it (one POST to `/title`, none to `/send`) and after the reload the field and the open chat's entry in the left list show it; Escape puts the saved title back, takes the focus away and hides Save again; a title of blanks is refused with the sentence under the field and what was typed kept; an empty field is stopped by the browser (nothing posted); screenshots of the column idle, focused and refused when a folder is given;
- the right column: the model picker is in it; Hide gives the conversation its width (two grid columns), the handle covers no part of the conversation or the answer box, the state survives a reload (and a click elsewhere on the page does not show it again), the handle shows it again; the sandbox's facts stay open after a reload once opened;
- the side columns' edges: a mouse drag of the left edge by +80 px and of the right one by -80 px widens each by about that, kept after a reload; a double-click gives the default width back; a drag past the maximum stops at it; a drag that would leave the conversation under 400 px stops there; the arrow keys change a focused edge's width by 16 px;
- the left column: Hide gives the conversation its width, the handle "Chats" covers no part of the conversation and shows it again, the state survives a reload; both columns hidden at once leave one grid column; at 1000 px wide no edge, no left Hide button and no left handle is shown and the left column is drawn although hidden above.

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
RIGHT_HANDLE = ".panel-handle[data-chat-panel]"
LEFT_HANDLE = ".panel-handle[data-chat-left]"
WIDTH = "(selector) => document.querySelector(selector).getBoundingClientRect().width"
COLUMNS = "() => getComputedStyle(document.querySelector('.cw')).gridTemplateColumns.split(' ').length"
AT_BOTTOM = "() => { const box = document.querySelector('.cw .convo'); return box.scrollHeight - box.scrollTop - box.clientHeight <= 40; }"
BOX_SEEN = "() => { const box = document.querySelector('.cw .convo').getBoundingClientRect(); const answer = document.querySelector('.cw .convo .composer').getBoundingClientRect(); return answer.top >= box.top && answer.bottom <= box.bottom + 1; }"
MID_STILL = "() => { const mid = document.querySelector('.cw .mid'); return mid.scrollHeight <= mid.clientHeight + 1; }"
UNDER_HANDLE = """(which) => {
  const handle = document.querySelector('.panel-handle[' + which + ']').getBoundingClientRect();
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
            for path, name in (("/overview", "overview"), (source, "source"), ("/queue", "queue"), ("/chat", "chat"), (chat, "chat-conversation"), (pending, "confirm")):
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

        # ── a generated title ──
        def new_chat_asked(question):
            """A new chat, its first question sent; the page after the send's reload, marked so that a later reload shows."""
            page.goto(base + "chat")
            page.wait_for_load_state("networkidle")
            page.click("aside.left form[action='/chat/new'] button")
            page.wait_for_load_state("networkidle")
            page.fill(".cw form.send textarea", question)
            with page.expect_navigation():
                page.press(".cw form.send textarea", "Enter")
            page.wait_for_load_state("networkidle")
            page.evaluate("window.__sdpMark = 'not reloaded'")

        def label():
            return page.evaluate("document.querySelector('aside.left .navi.on .lbl').textContent")

        def titled(question):
            """Wait until the open chat's label in the list is no longer the question's start: the generated title came."""
            return wait_until("(question) => { const label = document.querySelector('aside.left .navi.on .lbl'); return label !== null && label.textContent !== question; }", question, 60)

        title_field = page.locator("#chat-title")
        question = "Where does the data come from (title check)?"
        new_chat_asked(question)
        expect(label() == question and title_field.input_value() == question, "after the first question the list and the field show its start")
        expect(titled(question), f"the generated title came into the chat list ({label()!r})")
        generated = label()
        expect(page.evaluate("window.__sdpMark") == "not reloaded", "without a reload")
        expect(wait_until("(title) => document.getElementById('chat-title').value === title", generated, 5) and title_field.get_attribute("data-saved") == generated and not page.evaluate("document.querySelector('.chat-title-form').classList.contains('changed')"), f"the title's field shows it as its saved title ({title_field.input_value()!r})")
        expect(page.locator(".cw .chat .chat-unknown").count() == 0 and generated not in page.inner_text(".cw .chat"), "the conversation shows nothing of it and no line it does not understand")
        expect(wait_until("() => document.querySelectorAll('.cw .chat .chat-end').length > 0", None, 60), "the turn ended")
        expect(wait_until("() => { const part = document.getElementById('chat-totals'); return part !== null && part.innerText.includes('(includes the title)'); }", None, 20), f"the turn totals' Cost includes the title ({page.inner_text('#chat-totals')!r})")
        if shots:
            page.screenshot(path=os.path.join(shots, f"title-generated-{theme()}.png"))
        question = "Which sources are loaded (focus check)?"
        new_chat_asked(question)
        title_field.click()
        expect(titled(question), "with the field focused, the generated title still came into the chat list")
        expect(title_field.input_value() == question and page.evaluate("document.activeElement.id") == "chat-title" and title_field.get_attribute("data-saved") == question, f"a focused field is left as it is ({title_field.input_value()!r})")
        generated = label()
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(title_field.input_value() == generated, "a reload shows the generated title in the field")
        question = "How large is each source (edit check)?"
        new_chat_asked(question)
        title_field.fill("My own draft")
        page.evaluate("document.activeElement.blur()")
        expect(titled(question), "with an edit in the field, the generated title still came into the chat list")
        expect(title_field.input_value() == "My own draft" and page.evaluate("document.querySelector('.chat-title-form').classList.contains('changed')"), f"an edited field is left as it is ({title_field.input_value()!r})")
        violations()

        # ── the chat's title ──
        page.goto(base + chat.lstrip("/"))
        page.wait_for_load_state("networkidle")
        field, save = page.locator("#chat-title"), page.locator(".chat-title-form .title-save")
        posts = []
        page.on("request", lambda request: posts.append(urllib.parse.urlsplit(request.url).path) if request.method == "POST" else None)
        expect(page.locator("aside.rightp form.chat-title-form #chat-title").count() == 1 and page.locator("form.send #chat-title").count() == 0, "the title's field is in This chat, in its own form")
        idle = page.evaluate("() => { const style = getComputedStyle(document.getElementById('chat-title')); return [style.borderTopColor, style.backgroundColor]; }")
        expect(save.is_hidden() and idle == ["rgba(0, 0, 0, 0)", "rgba(0, 0, 0, 0)"], f"idle, the title has no box and no Save button ({idle})")
        if shots:
            page.locator("aside.rightp").screenshot(path=os.path.join(shots, f"title-idle-{theme()}.png"))
        field.click()
        expect(save.is_visible(), "focused, the title shows its Save button")
        if shots:
            page.locator("aside.rightp").screenshot(path=os.path.join(shots, f"title-focused-{theme()}.png"))
        renamed = "Renamed by the browser check"
        field.fill(renamed)
        with page.expect_navigation():
            field.press("Enter")
        page.wait_for_load_state("networkidle")
        expect(posts.count(urllib.parse.urlsplit(page.url).path + "/title") == 1 and not any(path.endswith("/send") for path in posts), f"Enter in the title posts the title once and never the question ({posts})")
        expect(field.input_value() == renamed and page.inner_text("aside.left .navi.on .lbl") == renamed, "after the save the field and the chat list show the new title")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(field.input_value() == renamed and save.is_hidden(), "the title is kept after a reload")
        field.click()
        field.fill("not to be kept")
        expect(page.evaluate("document.querySelector('.chat-title-form').classList.contains('changed')"), "an edited title marks its form changed")
        field.press("Escape")
        expect(field.input_value() == renamed and page.evaluate("document.activeElement.id") != "chat-title" and save.is_hidden(), "Escape puts the saved title back, leaves the field and hides Save")
        before_posts = len(posts)
        field.fill("")
        field.press("Enter")
        page.wait_for_timeout(600)
        expect(len(posts) == before_posts and page.evaluate("document.getElementById('chat-title').validity.valueMissing"), "an empty field is stopped by the browser: nothing is posted")
        field.fill("   ")
        known = len(problems)
        with page.expect_navigation():
            field.press("Enter")
        page.wait_for_load_state("networkidle")
        # The refusal is answered 400 on purpose, which Chromium logs as a console error of the page: that one line is expected, any other is not.
        refused = "console error: Failed to load resource: the server responded with a status of 400 (Bad Request)"
        expect(problems[known:].count(refused) == 1, "the refused title was answered 400 once")
        problems[known:] = [problem for problem in problems[known:] if problem != refused]
        line = page.locator("#chat-title-line")
        expect(line.count() == 1 and "1 to 80 characters" in line.inner_text() and field.input_value() == "   " and save.is_visible(), f"a title of blanks is refused with the sentence under the field, the typed text kept ({line.inner_text() if line.count() else 'no line'!r})")
        expect(page.inner_text("aside.left .navi.on .lbl") == renamed, "the refused title left the saved one in the list")
        if shots:
            page.locator("aside.rightp").screenshot(path=os.path.join(shots, f"title-refused-{theme()}.png"))
        violations()
        # Back to the chat by a GET: a reload of the refused page would post the title again.
        page.goto(base + chat.lstrip("/"))
        page.wait_for_load_state("networkidle")

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
        expect(page.evaluate(COLUMNS) == 2 and page.locator("aside.rightp").is_hidden() and page.locator(RIGHT_HANDLE).is_visible(), "Hide gives the conversation the width; the handle is shown")
        for where in ("0", "document.querySelector('.cw .convo').scrollHeight"):
            page.evaluate(f"document.querySelector('.cw .convo').scrollTop = {where}")
            expect(page.evaluate(UNDER_HANDLE, "data-chat-panel") == 0, f"the handle covers no part of the conversation (scrolled to {where})")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(page.evaluate(COLUMNS) == 2 and page.get_attribute(RIGHT_HANDLE, "aria-expanded") == "false", "the column stays hidden after a reload")
        page.mouse.click(700, 300)
        expect(page.evaluate(COLUMNS) == 2 and page.evaluate("localStorage.getItem('sdp.chatPanel')") == "closed", "a click elsewhere on the page leaves it hidden")
        page.click(RIGHT_HANDLE)
        expect(page.evaluate(COLUMNS) == 3 and page.locator("aside.rightp").is_visible(), "the handle shows the column again")
        page.click("details.sandbox > summary")
        # The details' toggle event, which keeps the state, comes a moment after the click.
        wait_until("() => localStorage.getItem('sdp.sandboxOpen') === '1'", None, 5)
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(page.evaluate("document.querySelector('details.sandbox').open") is True, "the sandbox's facts stay open after a reload")
        page.click("details.sandbox > summary")
        violations()

        # ── the side columns' widths ──
        def reset_look():
            page.evaluate("['sdp.chatLeft', 'sdp.chatPanel', 'sdp.chatLeftWidth', 'sdp.chatRightWidth'].forEach(function (key) { localStorage.removeItem(key); })")
            page.reload()
            page.wait_for_load_state("networkidle")

        def width(side):
            return page.evaluate(WIDTH, "aside.left" if side == "left" else "aside.rightp")

        def kept(side):
            return page.evaluate(f"localStorage.getItem('sdp.chat{side.title()}Width')")

        def drag(side, by):
            edge = page.locator(f".cw .edge-{side}").bounding_box()
            x, y = edge["x"] + edge["width"] / 2, edge["y"] + edge["height"] / 2
            page.mouse.move(x, y)
            page.mouse.down()
            page.mouse.move(x + by / 2, y, steps=4)
            dragging = page.evaluate("document.documentElement.classList.contains('col-dragging')")
            page.mouse.move(x + by, y, steps=4)
            page.mouse.up()
            return dragging

        def near(value, wanted):
            return abs(value - wanted) <= 2

        reset_look()
        expect(near(width("left"), 236) and near(width("right"), 330) and page.locator(".cw .col-edge").count() == 2, f"default widths 236 and 330 ({width('left')}, {width('right')})")
        dragging = drag("left", 80)
        expect(dragging and not page.evaluate("document.documentElement.classList.contains('col-dragging')"), "a drag marks the root while it runs, and only then")
        expect(near(width("left"), 316) and kept("left") == "316" and page.get_attribute(".cw .edge-left", "aria-valuenow") == "316", f"the left edge dragged by +80 px widens the column to 316 ({width('left')}, kept {kept('left')})")
        drag("right", -80)
        expect(near(width("right"), 410) and kept("right") == "410" and page.get_attribute(".cw .edge-right", "aria-valuenow") == "410", f"the right edge dragged by -80 px widens the column to 410 ({width('right')}, kept {kept('right')})")
        expect(page.evaluate("window.getSelection().toString()") == "", "nothing was selected by the drags")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(near(width("left"), 316) and near(width("right"), 410) and page.get_attribute(".cw .edge-left", "aria-valuenow") == "316", "both widths are kept after a reload")
        if shots:
            page.screenshot(path=os.path.join(shots, "chat-columns-wide.png"))
        page.dblclick(".cw .edge-left")
        page.dblclick(".cw .edge-right")
        expect(near(width("left"), 236) and near(width("right"), 330) and kept("left") is None and kept("right") is None, "a double-click gives each column its default width and forgets the kept one")
        drag("left", 400)
        expect(near(width("left"), 480) and kept("left") == "480", f"a drag past the left column's maximum stops at 480 ({width('left')})")
        room = page.evaluate("document.querySelector('.cw').clientWidth") - 400 - 480
        drag("right", -400)
        expect(near(width("right"), min(640, room)) and page.evaluate(WIDTH, ".cw .mid") >= 399, f"a drag that would leave the conversation under 400 px stops there (right {width('right')}, wanted {min(640, room)}, conversation {page.evaluate(WIDTH, '.cw .mid')})")
        page.dblclick(".cw .edge-left")
        page.dblclick(".cw .edge-right")
        page.focus(".cw .edge-left")
        page.keyboard.press("ArrowRight")
        page.keyboard.press("ArrowRight")
        page.keyboard.press("ArrowLeft")
        expect(near(width("left"), 252) and kept("left") == "252", f"right and left arrows on the left edge: +16, +16, -16 ({width('left')})")
        page.focus(".cw .edge-right")
        page.keyboard.press("ArrowLeft")
        expect(near(width("right"), 346) and kept("right") == "346", f"the left arrow on the right edge widens it by 16 ({width('right')})")
        page.keyboard.press("ArrowRight")
        page.keyboard.press("ArrowRight")
        expect(near(width("right"), 314) and kept("right") == "314", f"the right arrow on the right edge narrows it by 16 ({width('right')})")
        if shots:
            reset_look()
            page.evaluate("localStorage.setItem('sdp.chatLeftWidth', '180'); localStorage.setItem('sdp.chatRightWidth', '260')")
            page.reload()
            page.wait_for_load_state("networkidle")
            page.screenshot(path=os.path.join(shots, "chat-columns-narrow.png"))
        page.evaluate("localStorage.setItem('sdp.chatLeftWidth', '9999'); localStorage.setItem('sdp.chatRightWidth', '300.5')")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(near(width("left"), 236) and near(width("right"), 330), "kept widths out of range or not whole are ignored")
        violations()

        # ── the left column ──
        reset_look()
        page.click("[data-chat-left='close']")
        expect(page.evaluate(COLUMNS) == 2 and page.locator("aside.left").is_hidden() and page.locator(LEFT_HANDLE).is_visible() and page.locator(".cw .edge-left").is_hidden(), "Hide gives the conversation the left column's width; the handle is shown, the edge hidden")
        expect(page.evaluate("document.activeElement.matches('.panel-handle[data-chat-left]')"), "the focus moves to the handle")
        for where in ("0", "document.querySelector('.cw .convo').scrollHeight"):
            page.evaluate(f"document.querySelector('.cw .convo').scrollTop = {where}")
            expect(page.evaluate(UNDER_HANDLE, "data-chat-left") == 0, f"the left handle covers no part of the conversation (scrolled to {where})")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(page.evaluate(COLUMNS) == 2 and page.locator("aside.left").is_hidden() and page.get_attribute(LEFT_HANDLE, "aria-expanded") == "false", "the left column stays hidden after a reload")
        if shots:
            page.screenshot(path=os.path.join(shots, "chat-left-hidden.png"))
        page.click("[data-chat-panel='close']")
        expect(page.evaluate(COLUMNS) == 1 and page.locator(LEFT_HANDLE).is_visible() and page.locator(RIGHT_HANDLE).is_visible() and page.locator(".cw .col-edge:visible").count() == 0, "both columns hidden: one grid column, both handles, no edge")
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(page.evaluate(COLUMNS) == 1, "both stay hidden after a reload")
        for which in ("data-chat-left", "data-chat-panel"):
            expect(page.evaluate(UNDER_HANDLE, which) == 0, f"with both hidden the handle [{which}] covers no part of the conversation")
        if shots:
            page.screenshot(path=os.path.join(shots, "chat-both-hidden.png"))
        page.click(LEFT_HANDLE)
        expect(page.evaluate(COLUMNS) == 2 and page.locator("aside.left").is_visible() and page.evaluate("localStorage.getItem('sdp.chatLeft')") == "open", "the left handle shows the left column again")
        page.click(RIGHT_HANDLE)
        expect(page.evaluate(COLUMNS) == 3, "and the right one shows This chat")
        page.evaluate("localStorage.setItem('sdp.chatLeft', 'closed'); localStorage.setItem('sdp.chatLeftWidth', '400'); localStorage.setItem('sdp.chatRightWidth', '600')")
        page.set_viewport_size({"width": 1000, "height": 900})
        page.reload()
        page.wait_for_load_state("networkidle")
        expect(page.evaluate(COLUMNS) == 1 and page.locator("aside.left").is_visible() and near(width("left"), page.evaluate(WIDTH, ".cw .mid")), "at 1000 px the left column is stacked at full width although kept hidden and narrow")
        expect(page.locator(".cw .col-edge:visible").count() == 0 and page.locator(".cw .left-hide").is_hidden() and page.locator(LEFT_HANDLE).is_hidden(), "at 1000 px no edge, no left Hide button and no left handle")
        if shots:
            page.screenshot(path=os.path.join(shots, "chat-1000.png"))
        page.set_viewport_size({"width": 1440, "height": 900})
        reset_look()
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
