#!/usr/bin/env python3
"""A check of the web UI in a real browser, run by hand against a running page: `PYTHON tests/e2e/manual/check_webui_browser.py https://127.0.0.1:<port>/` with the management password on standard input, where PYTHON has Playwright and its Chromium, and `PLAYWRIGHT_BROWSERS_PATH` is set in the environment to the folder of its browsers (it has no default). `check_webui.py --browser PYTHON` runs it against the page it starts.

It is not collected by pytest. In headless Chromium (the self-signed certificate accepted for this one context) it loads the login page, logs in through the form, loads the Overview and the placeholder page of Sources, and logs out through the button. It fails on any Content-Security-Policy violation (the `securitypolicyviolation` event, collected by a script Playwright adds to every page before the page's own), any console error, any failed request of the page's own origin other than the two vendored files when they are missing, and on a page that is not the one expected after each step. It prints what it saw. Standard library and Playwright only.
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


def main():
    if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        print("PLAYWRIGHT_BROWSERS_PATH must be set to the folder of Playwright's browsers", file=sys.stderr)
        return 2
    base = sys.argv[1]
    password = sys.stdin.readline().rstrip("\n")
    origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlsplit(base))
    problems = []
    seen = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(ignore_https_errors=True)
        context.add_init_script(WATCH)
        page = context.new_page()
        page.on("console", lambda message: problems.append(f"console {message.type}: {message.text}") if message.type == "error" else None)
        page.on("requestfailed", lambda request: problems.append(f"request failed: {request.url} {request.failure}") if request.url.startswith(origin) else None)
        page.on("response", lambda response: seen.append((response.status, response.url)))

        def violations():
            found = page.evaluate("window.__sdpViolations || []")
            problems.extend(f"CSP violation on {page.url}: {entry}" for entry in found)
            return found

        page.goto(base + "login")
        print(f"  login page: {page.title()!r}, htmx loaded: {page.evaluate('typeof window.htmx')}")
        violations()
        page.fill("input[name=password]", password)
        page.click("button[type=submit]")
        page.wait_for_load_state("networkidle")
        if urllib.parse.urlsplit(page.url).path != "/" or "Overview" not in page.title():
            problems.append(f"after the login the page is {page.url} ({page.title()!r})")
        print(f"  after the login: {page.url}, title {page.title()!r}, the source demo listed: {page.locator('a[href=\"/sources/demo\"]').count() == 1}")
        violations()
        page.click("a[href='/sources']")
        page.wait_for_load_state("networkidle")
        print(f"  Sources: {page.locator('p.notice').first.inner_text()!r}")
        violations()
        page.goto(base)
        page.click("form.logout button")
        page.wait_for_load_state("networkidle")
        if urllib.parse.urlsplit(page.url).path != "/login":
            problems.append(f"after the logout the page is {page.url}")
        print(f"  after the logout: {page.url}, line {page.locator('p.notice').first.inner_text()!r}")
        violations()
        cookies = [cookie["name"] for cookie in context.cookies()]
        print(f"  cookies left: {cookies}")
        if cookies:
            problems.append(f"cookies left after the logout: {cookies}")
        browser.close()
    statuses = sorted({f"{status} {urllib.parse.urlsplit(url).path}" for status, url in seen})
    print("  answers seen: " + ", ".join(statuses))
    for problem in problems:
        print(f"  PROBLEM: {problem}")
    if problems:
        return 1
    print("  no CSP violation, no console error, no failed request")
    return 0


if __name__ == "__main__":
    sys.exit(main())
