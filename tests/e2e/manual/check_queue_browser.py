#!/usr/bin/env python3
"""A check of the web UI's Queue window in a real browser, run by hand against a running page: `PYTHON tests/e2e/manual/check_queue_browser.py https://127.0.0.1:<port>/ <job id> <the statement's length>` with the management password on standard input, where PYTHON has Playwright and its Chromium, and `PLAYWRIGHT_BROWSERS_PATH` is set in the environment to the folder of its browsers. `check_queue.py --browser PYTHON` runs it against the page it starts, with a pending job of the scheduler.

It is not collected by pytest. In headless Chromium (the self-signed certificate accepted for this one context), by clicking: it logs in through the form, opens the Queue from the window bar, clicks the job's row on the Pending tab, which opens the floating window (a modal dialog) whose statement box must hold exactly as many characters as the statement (a browser drops a line break right after `<pre>`, which the page writes for it); clicks EXPLAIN in the window and sees a plan or the database's refusal there; closes the window with Escape (it is emptied); clicks Approve on the job's row and sees the row's action cell say "approved by you" with no navigation (the address stays the Queue's); then it opens the Running tab and sees it ask for itself again (at least two answers of its part within 13 seconds, the address unchanged), and logs out. It fails on any Content-Security-Policy violation (collected by a script Playwright adds to every page before the page's own), any console error and any failed request of the page's own origin (a poll that a navigation of the page itself cuts short, `net::ERR_ABORTED`, is listed apart and not counted). Standard library and Playwright only.
"""

import os
import sys
import time
import urllib.parse

from playwright.sync_api import sync_playwright

WATCH = """
window.__sdpViolations = [];
document.addEventListener('securitypolicyviolation', function (event) {
  window.__sdpViolations.push(event.violatedDirective + ' ' + event.blockedURI);
});
"""
WAIT_MS = 60000


def main():
    if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        print("PLAYWRIGHT_BROWSERS_PATH must be set to the folder of Playwright's browsers", file=sys.stderr)
        return 2
    base, job_id, length = sys.argv[1], sys.argv[2], int(sys.argv[3])
    password = sys.stdin.readline().rstrip("\n")
    origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlsplit(base))
    problems = []
    aborted = []
    seen = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(ignore_https_errors=True)
        context.add_init_script(WATCH)
        page = context.new_page()
        page.on("console", lambda message: problems.append(f"console {message.type}: {message.text}") if message.type == "error" else None)

        def failed(request):
            if not request.url.startswith(origin):
                return
            if request.failure == "net::ERR_ABORTED":
                aborted.append(urllib.parse.urlsplit(request.url).path)
            else:
                problems.append(f"request failed: {request.url} {request.failure}")

        page.on("requestfailed", failed)
        page.on("response", lambda response: seen.append((time.monotonic(), response.status, urllib.parse.urlsplit(response.url).path)))

        def violations():
            problems.extend(f"CSP violation on {page.url}: {entry}" for entry in page.evaluate("window.__sdpViolations || []"))

        def expect(condition, what):
            if not condition:
                problems.append(what)

        page.goto(base + "login")
        page.fill("input[name=password]", password)
        page.click("button[type=submit]")
        page.wait_for_load_state("networkidle")
        violations()
        page.click("nav.wins a.win[href='/queue']")
        page.wait_for_load_state("networkidle")
        print(f"  the Queue from the window bar: {page.title()!r}, the job listed: {page.locator(f'a[href=\"/queue/{job_id}\"]').count() == 1}")
        violations()
        row = f"tr.job:has(a[href='/queue/{job_id}'])"
        navigations = []
        page.main_frame.on("framenavigated", lambda frame: navigations.append(frame.url))
        # A click on the row's description cell (no link, button or field there) opens the job's floating window.
        page.click(f"{row} td:nth-child(6)")
        page.wait_for_selector("#job-panel-body pre.statement", timeout=WAIT_MS)
        opened = page.evaluate("document.getElementById('job-panel').open")
        shown_length = page.evaluate("document.querySelector('#job-panel pre.statement').textContent.length")
        print(f"  the row opens the window: {opened}, its statement: {page.inner_text('#job-panel pre.statement')!r}, {shown_length} characters of {length}")
        expect(opened, "a click on the job's row did not open the window")
        expect(shown_length == length, f"the window's statement box holds {shown_length} characters, the statement {length}")
        violations()
        page.click("#job-panel form.act:has(input[name=action][value='queue.explain']) button[type=submit]")
        page.wait_for_selector(f"#pexp-{job_id} pre.output, #pexp-{job_id} [role=alert], #pexp-{job_id} .small", timeout=WAIT_MS)
        explained = page.inner_text(f"#pexp-{job_id}").strip()
        print(f"  EXPLAIN in the window: {explained.splitlines()[0] if explained else ''!r}")
        expect(explained != "", "EXPLAIN showed nothing in the window")
        violations()
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
        closed = not page.evaluate("document.getElementById('job-panel').open") and page.locator("#job-panel-body > *").count() == 0
        print(f"  Escape closes the window and empties it: {closed}")
        expect(closed, "Escape did not close and empty the window")
        page.click(f"#jact-{job_id} form.act:has(input[name=action][value='queue.approve']) button[type=submit]")
        page.wait_for_selector(f"#jact-{job_id} .pill, #jact-{job_id} [role=alert], #jact-{job_id} .small", timeout=WAIT_MS)
        cell = page.inner_text(f"#jact-{job_id}").strip()
        print(f"  Approve on the row: {cell!r}, navigations: {navigations}, the address: {page.url}")
        expect("approved by you" in cell, f"the row's action cell does not say approved by you: {cell!r}")
        expect(navigations == [] and urllib.parse.urlsplit(page.url).path == "/queue", f"Approve on the row navigated: {navigations} {page.url}")
        violations()
        page.goto(base + "queue?tab=running")
        page.wait_for_load_state("domcontentloaded")
        running = page.url
        started = time.monotonic()
        # Playwright's own wait: it hands the browser's events to the handlers while it waits, which a plain sleep would not.
        page.wait_for_timeout(13000)
        polls = [entry for entry in seen if entry[0] >= started and entry[2] == "/queue/running-rows"]
        print(f"  the running tab asked for itself {len(polls)} times in 13 seconds ({sorted({status for _, status, _ in polls})}), its address unchanged: {page.url == running}")
        expect(len(polls) >= 2 and all(status == 200 for _, status, _ in polls) and page.url == running, "the running tab did not refresh itself")
        violations()
        page.click("form.logout button")
        page.wait_for_load_state("networkidle")
        expect(urllib.parse.urlsplit(page.url).path == "/login", f"after the logout the page is {page.url}")
        violations()
        browser.close()
    if aborted:
        print(f"  polls cut short by a navigation of the page itself (not counted): {len(aborted)}")
    for problem in problems:
        print(f"  PROBLEM: {problem}")
    if problems:
        return 1
    print("  no CSP violation, no console error, no failed request")
    return 0


if __name__ == "__main__":
    sys.exit(main())
