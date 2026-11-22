#!/usr/bin/env python3
"""A check of the web UI's Chat window in a real browser, run by hand against a running page: `PYTHON tests/e2e/manual/check_chat_page_browser.py https://127.0.0.1:<port>/` with the management password on standard input, where PYTHON has Playwright and its Chromium, and `PLAYWRIGHT_BROWSERS_PATH` is set to the folder of its browsers. `check_chat_page.py --browser PYTHON` runs it against the chat it starts, whose stand-in API answers a question holding `PLOT-TURN` with a shell command writing `plot.png`.

It is not collected by pytest. In headless Chromium (the self-signed certificate accepted for this one context), by clicking: it logs in, opens Chat from the window bar (its first entry), makes a new chat, types the question and sends it, and then, without a reload (a mark set on the page after the send must still be there), waits for the events to appear: the question, the tool call collapsed, the answer and the image, which must have loaded (`naturalWidth` above 0). It deletes the chat through its confirmation and logs out. It fails on any Content-Security-Policy violation, any console error and any failed request of the page's own origin. Standard library and Playwright only.
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
LOADED = "() => { const image = document.querySelector('img.chat-image'); return image !== null && image.complete && image.naturalWidth > 0; }"


def main():
    if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        print("PLAYWRIGHT_BROWSERS_PATH must be set to the folder of Playwright's browsers", file=sys.stderr)
        return 2
    base = sys.argv[1]
    password = sys.stdin.readline().rstrip("\n")
    origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlsplit(base))
    problems = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(ignore_https_errors=True)
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

        page.goto(base + "login")
        page.fill("input[name=password]", password)
        page.click("button[type=submit]")
        page.wait_for_load_state("networkidle")
        expect(page.get_attribute("nav.windows a >> nth=0", "href") == "/chat", "Chat is the first entry of the window bar")
        page.click("nav.windows a[href='/chat']")
        page.wait_for_load_state("networkidle")
        violations()
        page.click("form[action='/chat/new'] button")
        page.wait_for_load_state("networkidle")
        chat = urllib.parse.urlsplit(page.url).path
        expect(chat.startswith("/chat/c_"), f"New chat leads to {chat}")
        page.fill("textarea[name=text]", "PLOT-TURN by clicking")
        page.click("form[action$='/send'] button")
        page.wait_for_load_state("domcontentloaded")
        page.evaluate("window.__sdpMark = 'not reloaded'")
        page.wait_for_function(LOADED, timeout=180000)
        page.wait_for_selector("p.chat-end", timeout=60000)
        expect(page.evaluate("window.__sdpMark") == "not reloaded", "the events appeared without a reload")
        shown = page.inner_text("section.chat-log")
        expect("PLOT-TURN by clicking" in shown and "Here is the plot: plot.png" in shown, "the question and the answer are shown")
        expect(page.locator("details.chat-tool").count() >= 2 and page.locator("details.chat-tool[open]").count() == 0, "the tool call and its result are collapsed")
        expect(page.evaluate("document.querySelector('img.chat-image').naturalWidth") > 0, "the image element loaded (naturalWidth > 0)")
        expect(page.locator("text=Stop the turn").count() == 0, "the stop button is gone once the turn has ended")
        violations()
        page.click(f"a[href='{chat}/delete']")
        page.wait_for_load_state("networkidle")
        page.click(f"form[action='{chat}/delete'] button")
        page.wait_for_load_state("networkidle")
        expect(urllib.parse.urlsplit(page.url).path == "/chat" and page.locator(f"a[href='{chat}']").count() == 0, "the chat is deleted through its confirmation")
        violations()
        page.click("form.logout button")
        page.wait_for_load_state("networkidle")
        browser.close()
    if problems:
        for problem in problems:
            print(f"  PROBLEM: {problem}")
        return 1
    print("  no CSP violation, no console error, no failed request")
    return 0


if __name__ == "__main__":
    sys.exit(main())
